from kafka import KafkaProducer
import json
import os

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_message(topic, message):
    """전역 KafkaProducer를 사용해 메시지 전송"""
    producer.send(topic, message)
    producer.flush()