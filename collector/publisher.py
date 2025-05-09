import os
import json
from gateway.kafka_producer import send_message

def publish(topic: str, message: dict):
    try:
        send_message(topic, message)
    except Exception:
        backup(topic, message)

def backup(topic: str, message: dict):
    os.makedirs("backup", exist_ok=True)
    with open(f"backup/{topic}.jsonl", "a") as f:
        f.write(json.dumps(message) + "\n")
