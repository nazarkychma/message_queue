from config import Config
from flask import Flask, request, make_response, jsonify
from flask_jwt_extended import JWTManager, verify_jwt_in_request, get_jwt
from functools import wraps
import requests

app = Flask(__name__)
app.config['JWT_ALGORITHM'] = 'RS256'
app.config['JWT_PUBLIC_KEY'] = open('public_key.pem').read()
jwt = JWTManager(app)
config = Config()
messages = {}

def setup() -> None:
    response = requests.post(f"{config.main_node_url}/register", data={"port": config.port}).json()
    print(response)
    if response["leader"]:
        config.leader = True
    config.brokers = response["brokers"]
    print(config.brokers, config.leader)

def permission_required(permission_name):
    def required(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            verify_jwt_in_request()
            permissions = get_jwt()
            if not permissions[permission_name]:
                return jsonify(msg='Access denied!'), 403
            else:
                return fn(*args, **kwargs)
        return wrapper
    return required

@app.route("/publish", methods=["POST"])
@permission_required("can_produce")
def publish():
    message = request.form.get("message")
    print(message)
    if message is None:
        return make_response({"message": "message is required"}, 404)
    if config.leader:
        index = str(len(messages))
        messages[index] = message
        if len(config.brokers) > 1:
            requests.post(f"{config.main_node_url}/sync", json={"messages": messages})
        return {"status": "ok", "brokers": config.brokers}
    try:
        response = requests.get(f"{config.get_leader()}/health").json()
        if response["status"] == "ok":
            requests.post(f"{config.get_leader()}/publish", data=request.form)
        return {"status": "ok", "brokers": config.brokers}
    except requests.exceptions.ConnectionError:
        return make_response({"message": "leader is broken"}, 503)

@app.route("/assign", methods=["GET"])
def assign():
    config.leader = True
    return {"status": "ok"}

@app.route("/sync", methods=["POST"])
def sync():
    global messages
    updated_messages = request.get_json()["messages"]
    if updated_messages is None:
        return make_response({"message": "messages list is required"}, 404)
    messages = updated_messages
    return {"status": "ok"}

@app.route("/update_brokers", methods=["POST"])
def update_brokers():
    brokers = request.get_json()["brokers"]
    if brokers is None:
        return make_response({"message": "brokers list is required"}, 404)
    config.brokers = brokers
    return {"status": "ok"}

@app.route('/get', methods=["GET"])
@permission_required("can_consume")
def get_messages():
    consumer_id = request.form.get("consumer_id")
    index = request.form.get("index")
    if consumer_id is None:
        consumer_id = "Default"
    if index == "-1":
        response = requests.get(f"{config.main_node_url}/get_last_consumed_index/{consumer_id}").json()
        index = response["index"]
    message = messages.get(index)
    if message is None:
        return make_response({"message": "there is no message with such index"}, 409)
    requests.post(f"{config.main_node_url}/commit_index", data={"index": index, "consumer_id": consumer_id})
    return {index: message}

@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    setup()
    app.run(host="0.0.0.0", port=config.port)
    requests.post(f"{config.main_node_url}/deregister", data={"port": config.port})
