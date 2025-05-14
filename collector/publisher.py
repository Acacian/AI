import os
import json
import traceback
from kafka import KafkaProducer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    acks='all',       # 모든 브로커 확인 후 성공 처리
    retries=3         # 실패 시 재시도
)

def publish(topic: str, message: dict):
    try:
        future = producer.send(topic, message)
        result = future.get(timeout=10)  # 실제 전송이 끝날 때까지 대기
    except Exception as e:
        print(f"❌ Kafka 전송 실패: {e}", flush=True)
        print(traceback.format_exc())
        backup(topic, message)

def backup(topic: str, message: dict):
    os.makedirs("backup", exist_ok=True)
    with open(f"backup/{topic}.jsonl", "a") as f:
        f.write(json.dumps(message) + "\n")
    print(f"📦 로컬 백업 완료: {topic}", flush=True)