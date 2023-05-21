from config import Config
from flask import Flask, request, abort, make_response
import requests

app = Flask(__name__)
config = Config()
messages = {}

def setup() -> None:
    response = requests.post(f"{config.main_node_url}/register", data={"port": config.port}).json()
    if response["leader"]:
        config.leader = True
    config.brokers = response["brokers"]
    print(config.brokers, config.leader)


@app.route("/publish", methods=["POST"])
def publish():
    message = request.form.get("message")
    if message is None:
        return make_response({"message": "message is required"}, 404)
    if config.leader:
        index = len(messages)
        messages[index] = message
        requests.post(f"{config.main_node_url}/sync", data={"messages": messages})
        return {"status": "ok", "brokers": config.brokers}
    try:
        response = requests.get(f"{config.get_leader()}/health").json()
        if response["status"] == "ok":
            requests.post(f"{config.get_leader()}/publish", data=request.form)
        return {"status": "ok", "brokers": config.brokers}
    except requests.exceptions.ConnectionError:
        return make_response({"message": "leader is broken"}, 503)

@app.route("/assign", methods=["POST"])
def assign():
    config.leader = True
    return {"status": "ok"}

@app.route("/sync", methods=["POST"])
def sync():
    updated_messages = request.form.get("messages")
    if messages is None:
        abort(404)
    messages = updated_messages
    return {"status": "ok"}

@app.route("/update_brokers", methods=["POST"])
def update_brokers():
    brokers = request.form.get("brokers")
    if brokers is None:
        abort(404)
    config.brokers = brokers
    return {"status": "ok"}

@app.route('/get', methods=["GET"])
def get_messages():
    consumer_id = request.form.get("consumer_id")
    index = request.form.get("consumer_id")
    if consumer_id is None:
        consumer_id = "Default"
    if index == "-1":
        response = requests.get(f"{config.main_node_url}/get_last_consumed_index/{consumer_id}").json()
        index = response["index"]
    message = messages.get(index)
    if message is None:
        abort(404)
    request.post(f"{config.main_node_url}/commit_index", data={"index": index, "consumer_id": consumer_id})
    return {index: message}

@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    setup()
    app.run(host="0.0.0.0", port=config.port)
    requests.post(f"{config.main_node_url}/deregister", data={"port": config.port})