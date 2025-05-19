import os
import json
import traceback
import logging
from datetime import datetime
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ─────────────────────────────────────────────
# 로깅 레벨 환경변수 기반 설정
# ─────────────────────────────────────────────
LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()

level_map = {
    "CRITICAL": logging.CRITICAL,
    "ERROR": logging.ERROR,
    "WARNING": logging.WARNING,
    "INFO": logging.INFO,
    "DEBUG": logging.DEBUG,
}

logger = logging.getLogger("publisher")
logger.setLevel(level_map.get(LOG_LEVEL, logging.INFO))

handler = logging.StreamHandler()
formatter = logging.Formatter("[%(asctime)s] %(levelname)s | %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)

if LOG_LEVEL not in level_map:
    logger.warning(f"⚠️ 잘못된 LOG_LEVEL: '{LOG_LEVEL}', 기본 INFO 레벨로 설정됨")

# ─────────────────────────────────────────────
# Kafka 설정
# ─────────────────────────────────────────────
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")

try:
    producer = KafkaProducer(
        bootstrap_servers=KAFKA_BROKER,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
        acks='all',
        retries=3
    )
    logger.info(f"Kafka 연결 완료: {KAFKA_BROKER}")
except KafkaError as e:
    logger.error(f"Kafka 연결 실패: {e}")
    producer = None

# ─────────────────────────────────────────────
# 메시지 발행 함수
# ─────────────────────────────────────────────
def publish(topic: str, message: dict):
    if not producer:
        logger.warning(f"Kafka 사용 불가. 백업 처리됨: {topic}")
        backup(topic, message)
        return

    try:
        future = producer.send(topic, message)
        result = future.get(timeout=10)
        logger.debug(f"Kafka 전송 완료: {topic} | offset={result.offset}")
    except Exception as e:
        logger.error(f"Kafka 전송 실패: {e}")
        logger.debug(traceback.format_exc())
        backup(topic, message)

# ─────────────────────────────────────────────
# 백업 처리
# ─────────────────────────────────────────────
def backup(topic: str, message: dict):
    os.makedirs("backup", exist_ok=True)
    date_str = datetime.utcnow().strftime("%Y-%m-%d")
    file_path = os.path.join("backup", f"{topic}_{date_str}.jsonl")

    try:
        with open(file_path, "a") as f:
            f.write(json.dumps(message) + "\n")
        logger.info(f"로컬 백업 완료: {file_path}")
    except Exception as e:
        logger.error(f"로컬 백업 실패: {e}")
