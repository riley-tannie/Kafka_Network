import json
import os
import paramiko
from dotenv import load_dotenv
from kafka import KafkaConsumer, KafkaProducer

load_dotenv()

KAFKA_SERVER = "localhost:9092"
COMMAND_TOPIC = "router-commands"
RESULT_TOPIC = "router-results"

SSH_USERNAME = os.getenv("SSH_USERNAME")
SSH_PASSWORD = os.getenv("SSH_PASSWORD")

consumer = KafkaConsumer(
    COMMAND_TOPIC,
    bootstrap_servers=KAFKA_SERVER,
    auto_offset_reset="latest",
    enable_auto_commit=True,
    group_id="router-command-worker",
    value_deserializer=lambda x: json.loads(x.decode("utf-8"))
)

producer = KafkaProducer(
    bootstrap_servers=KAFKA_SERVER,
    value_serializer=lambda value: json.dumps(value).encode("utf-8")
)


def execute_router_command(router_ip, command, request_id):

    print("\n===================================")
    print(f"Request ID: {request_id}")
    print(f"Connecting to router: {router_ip}")
    print(f"Command: {command}")
    print("===================================")

    transport = None

    try:
        transport = paramiko.Transport((router_ip, 22))

        security = transport.get_security_options()

        security.kex = (
            "diffie-hellman-group14-sha1",
            "diffie-hellman-group-exchange-sha1",
            "diffie-hellman-group1-sha1",
        )

        security.ciphers = (
            "aes128-cbc",
            "aes192-cbc",
            "aes256-cbc",
            "3des-cbc",
        )

        security.digests = (
            "hmac-sha1",
            "hmac-sha1-96",
            "hmac-md5",
            "hmac-md5-96",
        )

        security.key_types = (
            "ssh-rsa",
            "rsa-sha2-512",
            "rsa-sha2-256",
        )

        print("Starting SSH negotiation...")

        transport.connect(
            username=SSH_USERNAME,
            password=SSH_PASSWORD
        )

        print("SSH connection successful!")

        channel = transport.open_session()
        channel.exec_command(command)

        output = channel.makefile("r").read()
        error = channel.makefile_stderr("r").read()

        if isinstance(output, bytes):
            output = output.decode("utf-8", errors="replace")

        if isinstance(error, bytes):
            error = error.decode("utf-8", errors="replace")

        print("\n--- ROUTER OUTPUT ---")
        print(output)

        if error:
            print("\n--- ROUTER ERROR ---")
            print(error)

        result = {
            "request_id": request_id,
            "router_ip": router_ip,
            "command": command,
            "status": "success",
            "output": output,
            "error": error
        }

        producer.send(RESULT_TOPIC, result)
        producer.flush()

        print("\nResult sent to Kafka.")

    except Exception as e:

        print(f"\nSSH connection failed: {e}")

        result = {
            "request_id": request_id,
            "router_ip": router_ip,
            "command": command,
            "status": "error",
            "output": "",
            "error": str(e)
        }

        producer.send(RESULT_TOPIC, result)
        producer.flush()

    finally:

        if transport:
            transport.close()


print("===================================")
print(" Router Command Worker")
print(" Waiting for Kafka messages...")
print("===================================")


for message in consumer:

    data = message.value

    router_ip = data.get("router_ip")
    command = data.get("command")
    request_id = data.get("request_id")

    if not router_ip or not command or not request_id:
        print("Invalid Kafka message:", data)
        continue

    execute_router_command(
        router_ip,
        command,
        request_id
    )