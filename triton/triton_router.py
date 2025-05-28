import os
import json
import yaml
import logging
from kafka import KafkaConsumer
from triton_client import TritonClient
from kafka_utils import send_message

# 환경 변수에서 로깅 레벨 설정
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=LOG_LEVEL,
    format="%(asctime)s | TritonRouter | %(levelname)s | %(message)s"
)
logger = logging.getLogger("TritonRouter")

KAFKA_BROKER = "kafka:9092"
CONFIG_PATH = "triton/config.yml"
GROUP_ID = "ai_triton_router_group"
DONE_DIR = "models"

triton = TritonClient()

with open(CONFIG_PATH, 'r') as f:
    config = yaml.safe_load(f)

topic_model_map = config.get("topics", {})
next_topic_map = config.get("routing", {})

# done 체크 캐시
model_ready_cache = set()
seen_unready_models = set()

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
    return (
        ("input" in data and isinstance(data["input"], list)) or
        ("bids" in data and "asks" in data)
    )

def is_model_ready(model_name: str, base_path: str = DONE_DIR) -> bool:
    if model_name in model_ready_cache:
        return True

    done_path = os.path.join(base_path, f"{model_name}.done")
    if os.path.exists(done_path):
        model_ready_cache.add(model_name)
        return True
    else:
        # ✅ 처음만 로그 출력
        if model_name not in seen_unready_models:
            logger.debug(f"⏸️ 모델 학습 미완료 (.done 없음): {model_name} → 메시지 무시")
            seen_unready_models.add(model_name)
        return False

def flatten_orderbook(bids, asks):
    def flatten(side): return [float(v) for pair in side[:20] for v in pair]
    return flatten(bids) + flatten(asks)

def consume_loop():
    topics = list(topic_model_map.keys())
    logger.info(f"📥 Kafka Consumer 구독 시작: {topics}")

    consumer = get_kafka_consumer(topics)

    for msg in consumer:
        topic = msg.topic
        data = msg.value
        model_name = topic_model_map.get(topic)

        if not model_name:
            logger.warning(f"⚠️ 알 수 없는 토픽: {topic}")
            continue

        if not is_model_ready(model_name):
            logger.debug(f"⏸️ 모델 학습 미완료 (.done 없음): {model_name} → 메시지 무시")
            continue

        if not is_valid_data(data):
            logger.warning(f"⚠️ 유효하지 않은 데이터 구조: {data}")
            continue

        try:
            # orderbook 형태 변환
            if "bids" in data and "asks" in data:
                input_data = {"input": [flatten_orderbook(data["bids"], data["asks"])]}
            else:
                input_data = data

            result = triton.infer(model_name, input_data)
            logger.info(f"✅ [{topic}] → {model_name} 결과: {result}")

            next_topic = next_topic_map.get(topic)
            if next_topic:
                send_message(next_topic, {"input": result[0]})
                logger.info(f"📤 결과 전송 → {next_topic}: {result[0]}")
            else:
                logger.info(f"🔚 최종 단계: {topic}")

        except Exception as e:
            logger.error(f"❌ 추론 실패: {topic} | {e}")
