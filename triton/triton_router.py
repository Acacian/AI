import json
import yaml
from kafka import KafkaConsumer
from triton.triton_client import TritonClient
from triton.kafka_utils import send_message

KAFKA_BROKER = "kafka:9092"
CONFIG_PATH = "triton/config.yml"
GROUP_ID = "ai_triton_router_group"

# Triton client 인스턴스
triton = TritonClient()

# Kafka 토픽-모델 매핑 및 라우팅 로드
with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

topic_model_map = config.get("topics", {})
next_topic_map = config.get("routing", {})

def get_kafka_consumer(topics):
    return KafkaConsumer(
        *topics,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        enable_auto_commit=True,
        group_id=GROUP_ID,
    )

def is_valid_data(data: dict) -> bool:
    return "input" in data and isinstance(data["input"], list)

def consume_loop():
    topics = list(topic_model_map.keys())
    consumer = get_kafka_consumer(topics)
    print(f"📥 Kafka Consumer 구독 시작: {topics}")

    for msg in consumer:
        topic = msg.topic
        data = msg.value

        model_name = topic_model_map.get(topic)
        if not model_name:
            print(f"⚠️ 알 수 없는 토픽: {topic}")
            continue

        if not is_valid_data(data):
            print(f"⚠️ 유효하지 않은 데이터 구조: {data}")
            continue

        try:
            result = triton.infer(model_name, data)
            print(f"✅ [{topic}] → {model_name} 결과: {result}")

            next_topic = next_topic_map.get(topic)
            if next_topic:
                send_message(next_topic, {"input": result[0]})
                print(f"📤 결과 전송 → {next_topic}: {result[0]}")
            else:
                print(f"🔚 최종 단계: {topic}")

        except Exception as e:
            print(f"❌ 추론 실패: {topic} | {e}")
