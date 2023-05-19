from flask import Flask, request
import requests
import os

app = Flask(__name__)

nodes = {}

@app.route("/register", methods=["POST"])
def register():
    sender_port = request.form.get("port")
    print(sender_port)
    sender_ip = request.remote_addr
    addr = f'http://{sender_ip}:{sender_port}'
    if addr not in nodes.keys():
        nodes[addr] = True if len(nodes) == 0 else False
    print(nodes)
    return {"brokers": nodes, "leader": nodes[addr]}

@app.route("/deregister", methods=["POST"])
def deregister():
    sender_ip = request.remote_addr
    sender_port = request.form.get("port")
    nodes.pop(f'{sender_ip}:{sender_port}')
    print(nodes)
    return nodes

@app.route("/get_last_consumed_index/<consumer>", methods=["GET"])
def get_last_consumed_index(consumer: str):
    pass

@app.route("/get_healthy_nodes", methods=["GET"])
def get_health_nodes():
    healthy_nodes = []
    for node in nodes:
        response = requests.get(f"{node}/health").json
        if response["status"] == "ok":
            healthy_nodes.append(node)
    return healthy_nodes

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=os.getenv("PORT"))