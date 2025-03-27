import json
import os
from kafka import KafkaConsumer
from gateway.triton_client import send_to_triton
from gateway.kafka_producer import send_message

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

TOPIC_MODEL_MAP = {
    "ai_mining_1m": "ai_mining",
    "ai_mining_5m": "ai_mining",
    "ai_mining_15m": "ai_mining",
    "ai_mining_1h": "ai_mining",
    "ai_mining_1d": "ai_mining",

    "ai_pattern_1m": "ai_pattern",
    "ai_pattern_5m": "ai_pattern",
    "ai_pattern_15m": "ai_pattern",
    "ai_pattern_1h": "ai_pattern",
    "ai_pattern_1d": "ai_pattern",

    "ai_risk_manage_1m": "ai_risk_manage",
    "ai_risk_manage_5m": "ai_risk_manage",
    "ai_risk_manage_15m": "ai_risk_manage",
    "ai_risk_manage_1h": "ai_risk_manage",
    "ai_risk_manage_1d": "ai_risk_manage",
}

NEXT_TOPIC_MAP = {
    "ai_mining_1m": "ai_pattern_1m",
    "ai_mining_5m": "ai_pattern_5m",
    "ai_mining_15m": "ai_pattern_15m",
    "ai_mining_1h": "ai_pattern_1h",
    "ai_mining_1d": "ai_pattern_1d",

    "ai_pattern_1m": "ai_risk_manage_1m",
    "ai_pattern_5m": "ai_risk_manage_5m",
    "ai_pattern_15m": "ai_risk_manage_15m",
    "ai_pattern_1h": "ai_risk_manage_1h",
    "ai_pattern_1d": "ai_risk_manage_1d",
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
    topics = list(TOPIC_MODEL_MAP.keys())
    consumer = get_kafka_consumer(topics)
    print(f"📥 Kafka Consumer 구독 시작: {topics}")

    for msg in consumer:
        topic = msg.topic
        data = msg.value

        model_name = TOPIC_MODEL_MAP.get(topic)
        if not model_name:
            print(f"⚠️ 알 수 없는 토픽: {topic}")
            continue

        # Triton Inference
        result = send_to_triton(model_name, data)
        print(f"✅ [{topic}] → {model_name} 결과: {result}")

        # 다음 에이전트로 메시지 전송
        next_topic = NEXT_TOPIC_MAP.get(topic)
        if next_topic:
            send_message(next_topic, {"input": result[0]})
            print(f"📤 결과 전송 → {next_topic}: {result[0]}")
        else:
            print(f"🔚 최종 단계 (다음 없음): {topic}")
