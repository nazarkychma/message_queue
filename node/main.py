from config import Config
from flask import Flask, request, abort
import requests

app = Flask(__name__)
config = Config()

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
        abort(404)
    if config.leader:
        return {"index": "ok"}

    # TODO: if leader, then save assign index to msg, send it main node
    # if isr, request current broker list from main node, redirect to leader
    # return broker list
    print(message)
    return {"status": "ok"}

@app.route("/sync", methods=["POST"])
def sync():
    index = request.form.get("index")
    message = request.form.get("message")
    if message is None or index is None:
        abort(404)
    # TODO: if leader, then save assign index to msg, send it main node
    # if isr, request current broker list from main node, redirect to leader
    # return broker list
    print(index, message)
    return {"status": "ok"}


@app.route('/get', methods=["GET"])
def get_messages():
    consumer_id = request.form.get("consumer_id")
    if consumer_id is None:
        consumer_id = "Default"
    response = requests.get()
    # TODO send request to main node and get last consumed index @ current topic, if id is new, start from 0
    # read message from file, update consumed index and return msg
    return {"index": "msg"}

@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    setup()
    app.run(host="0.0.0.0", port=config.port)
    requests.post(f"{config.main_node_url}/deregister", data={"port": config.port})