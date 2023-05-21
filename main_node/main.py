from flask import Flask, request, make_response
import requests
import os

app = Flask(__name__)

nodes = {}
consumed_indexes = {}

def batch_request(method, addr_book, endpoint, request_data={}):
    methods = {
        "GET": requests.get,
        "POST": requests.post
    }
    successful_response = []
    for addr in addr_book:
        try:
            response = methods[method](f"{addr}/{endpoint}", json=request_data)
            if response.status_code == 200:
                successful_response.append(addr)
        except:
            ...
    return successful_response

@app.route("/register", methods=["POST"])
def register():
    sender_port = request.form.get("port")
    print(sender_port)
    sender_ip = request.remote_addr
    addr = f'http://{sender_ip}:{sender_port}'
    if addr not in nodes.keys():
        curr_nodes = nodes.copy()
        nodes[addr] = True if len(nodes) == 0 else False
        batch_request("POST", curr_nodes.keys(), "update_brokers", {"brokers": nodes})
    print(nodes)
    return {"brokers": nodes, "leader": nodes[addr]}

@app.route("/deregister", methods=["POST"])
def deregister():
    sender_ip = request.remote_addr
    sender_port = request.form.get("port")
    nodes.pop(f'{sender_ip}:{sender_port}')
    print(nodes)
    batch_request("POST", nodes.keys(), "update_brokers", {"brokers": nodes})
    return nodes

@app.route("/get_last_consumed_index/<consumer>", methods=["GET"])
def get_last_consumed_index(consumer: str):
    index = consumed_indexes.get(consumer)
    if index is None:
        index = 0
        consumed_indexes[consumer] = index
    return {"index": str(index)}

@app.route("/commit_index", methods=["POST"])
def commit_index():
    consumer = request.form.get("consumer_id")
    index = int(request.form.get("index"))
    current_index = int(consumed_indexes.get(consumer))
    if current_index is None:
        consumed_indexes[consumer] = index
    if index > current_index:
        consumed_indexes[consumer] = index
    return {"status": "ok"}

@app.route("/sync", methods=["POST"])
def sync():
    messages = request.get_json()
    batch_request("POST", nodes.keys(), "sync", messages)
    return {"status": "ok"}

@app.route("/assign_new_leader", methods=["GET"])
def get_health_nodes():
    global nodes
    healthy_nodes = []
    for node in nodes.keys():
        try:
            response = requests.get(f"{node}/health").json()
            if response["status"] == "ok":
                healthy_nodes.append(node)
        except:
            ...
    if len(healthy_nodes) == 0:
        return make_response({"message": "there are 0 brokers available"}, 404)
    new_leader = healthy_nodes[0]
    for node in healthy_nodes:
        try:
            response = requests.get(f"{node}/assign").json()
            if response["status"] == "ok":
                new_leader = node
                break
        except requests.exceptions.ConnectionError:
            healthy_nodes.remove(node)
    nodes = {node:False for node in healthy_nodes}
    nodes[new_leader] = True
    batch_request("POST", nodes.keys(), "update_brokers", {"brokers": nodes})
    return {"brokers": nodes}

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=os.getenv("PORT"))
