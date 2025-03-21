from kafka import KafkaConsumer
import json
import os
from .triton_client import send_to_triton

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

# 🔧 topic → model_name 매핑
TOPIC_MODEL_MAP = {
    "ai_mining_1m": "ai_mining",
    "ai_mining_5m": "ai_mining",
    "ai_mining_15m": "ai_mining",
    "ai_mining_1h": "ai_mining",
    "ai_mining_1d": "ai_mining"
}

def get_kafka_consumer(topics):
    return KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id="ai_gateway_consumer_group"
    )

def consume_loop():
    consumer = get_kafka_consumer(list(TOPIC_MODEL_MAP.keys()))
    print("📥 Kafka Consumer 시작됨...")

    for msg in consumer:
        topic = msg.topic
        data = msg.value

        model_name = TOPIC_MODEL_MAP.get(topic)
        if not model_name:
            print(f"⚠️ 처리할 모델 없음: {topic}")
            continue

        result = send_to_triton(model_name, data)
        print(f"✅ [{topic}] Triton 응답: {result}")