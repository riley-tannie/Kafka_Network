from flask import Flask, request, jsonify
from flask_cors import CORS
from kafka import KafkaProducer, KafkaConsumer
import json
import uuid
import threading

app = Flask(__name__)
CORS(app)


# ==========================================
# Kafka Producer
# ==========================================

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


# ==========================================
# Store router results temporarily
# ==========================================

results = {}


# ==========================================
# Kafka Result Listener
# ==========================================

def listen_for_results():

    consumer = KafkaConsumer(
        "router-results",
        bootstrap_servers="localhost:9092",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="flask-result-listener",
        value_deserializer=lambda x: json.loads(x.decode("utf-8"))
    )

    print("Result listener started.")

    for message in consumer:

        data = message.value

        request_id = data.get("request_id")

        if request_id:

            results[request_id] = data

            print("\n===================================")
            print("Router result received")
            print(f"Request ID: {request_id}")
            print(f"Status: {data.get('status')}")
            print("===================================")


# ==========================================
# Start result listener
# ==========================================

result_thread = threading.Thread(
    target=listen_for_results,
    daemon=True
)

result_thread.start()


# ==========================================
# Home
# ==========================================

@app.route("/", methods=["GET"])
def home():

    return jsonify({
        "status": "success",
        "message": "Router Command API is running"
    })


# ==========================================
# Send Command
# ==========================================

@app.route("/command", methods=["POST"])
def send_command():

    data = request.get_json()

    if not data:

        return jsonify({
            "error": "JSON body is required"
        }), 400

    command = data.get("command")
    router_ip = data.get("router_ip")

    if not command:

        return jsonify({
            "error": "command is required"
        }), 400

    if not router_ip:

        return jsonify({
            "error": "router_ip is required"
        }), 400

    request_id = str(uuid.uuid4())

    message = {
        "request_id": request_id,
        "router_ip": router_ip,
        "command": command
    }

    producer.send(
        "router-commands",
        message
    )

    producer.flush()

    print("\n===================================")
    print("Command sent")
    print(f"Request ID: {request_id}")
    print(f"Router: {router_ip}")
    print(f"Command: {command}")
    print("===================================")

    return jsonify({
        "status": "queued",
        "request_id": request_id
    })


# ==========================================
# Get Result
# ==========================================

@app.route("/result/<request_id>", methods=["GET"])
def get_result(request_id):

    result = results.get(request_id)

    if result is None:

        return jsonify({
            "status": "pending",
            "request_id": request_id
        }), 202

    return jsonify(result)


# ==========================================
# Start Flask
# ==========================================

if __name__ == "__main__":

    app.run(
        host="0.0.0.0",
        port=5000,
        debug=False
    )