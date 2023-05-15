from flask import Flask, request, abort


app = Flask(__name__)

topic_dict = {}

@app.route("/publish/<topic>", methods=["POST"])
def publish(topic: str):
    message = request.form.get("message")
    replication_factor = request.form.get("min_replication_factor")
    if not message:
        abort(404)
    if topic not in topic_dict:
        topic_dict[topic] = []
    # TODO: get list of healthy nodes from main node, send msg to all of them
    # if 
    topic_dict[topic].append(message)
    print(topic, message, request.remote_addr)
    return {"status": "ok"}


@app.route('/get/<topic>', methods=["GET"])
def get_messages(topic: str):
    if topic not in topic_dict:
        abort(404)
    consumer_id = request.form.get("consumer_id")
    # TODO send request to main node and get last consumed index @ current topic, if id is new, start from 0
    # read message from file, update consumed index and return msg
    return topic_dict[topic]

@app.route("/health", methods=["GET"])
def health_check():
    return {"status": "ok"}

if __name__ == "__main__":
    app.run()