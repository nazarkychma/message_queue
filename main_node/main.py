from flask import Flask

app = Flask(__name__)

@app.route("/register")

@app.route("/deregister")

@app.route("/get_last_consumed_index")

@app.route("/get_healthy_nodes")

if __name__ == "__main__":
    app.run()