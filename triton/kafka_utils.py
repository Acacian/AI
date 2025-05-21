from kafka import KafkaProducer
import json

KAFKA_BROKER = "kafka:9092"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def send_message(topic: str, message: dict):
    """Kafka 메시지 전송"""
    producer.send(topic, message)
    producer.flush()