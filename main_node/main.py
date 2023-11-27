from flask import Flask, request, make_response, jsonify, abort
from flask_jwt_extended import JWTManager, create_access_token
from datetime import timedelta
from flask_bcrypt import Bcrypt
from flask_sqlalchemy import SQLAlchemy
import requests
import os

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = os.getenv("DB_URI")
app.config['JWT_ALGORITHM'] = 'RS256'
app.config['JWT_PRIVATE_KEY'] = open('private_key.pem').read()
#app.config['JWT_PUBLIC_KEY'] = os.getenv("RSA_PUBLIC_KEY")
#app.config['JWT_SECRET_KEY'] = 'your-secret-key'
db = SQLAlchemy(app)
jwt = JWTManager(app)
bcrypt = Bcrypt(app)

nodes = {}
consumed_indexes = {}

class Users(db.Model):
    __tablename__ = "users"
    login = db.Column(db.String(255), nullable=False, primary_key=True)
    password = db.Column(db.String(255), nullable=False)
    is_blocked = db.Column(db.Boolean, default=False)
    can_produce = db.Column(db.Boolean, default=True)
    can_consume = db.Column(db.Boolean, default=True)

    def verify_password(self, password):
        return bcrypt.check_password_hash(self.password, password.encode('utf-8'))

    def get_permissions(self):
        permissions = {
                'can_produce': self.can_produce,
                'can_consume': self.can_consume,
        }
        return permissions

@app.route('/login', methods=["POST"])
def login():
    if not request.json['login'] or not request.json['password']:
        abort(404)

    try:
        username = request.json.get('login')
        password = request.json.get('password')
        user = Users.query.filter_by(login=username).first_or_404()
        if user.verify_password(password) and not user.is_blocked:
            data = {
                'access_token': create_access_token(identity=username, additional_claims=user.get_permissions(), expires_delta=timedelta(minutes=5))
            }
            return jsonify(data), 200
        return 'Bad credentials', 403
    except Exception as e:
        return str(e), 404
        print(e)
        return 'Something went wrong', 404

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
