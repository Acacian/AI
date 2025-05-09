import os
import json
import yaml
import requests
from kafka import KafkaConsumer, KafkaProducer

# 설정 로딩
with open("agents_llm/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Kafka 설정
consumer = KafkaConsumer(
    config["listen_topic"],
    bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
    group_id=config.get("group_id", "llm_agent_group"),
    value_deserializer=lambda m: json.loads(m.decode("utf-8")),
    auto_offset_reset="latest",
)

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# 프롬프트 생성
def build_prompt(data: dict) -> str:
    try:
        return config["prompt_template"].format(**data)
    except KeyError as e:
        missing = str(e).strip("'")
        raise ValueError(f"프롬프트 생성 실패: 누락된 필드 '{missing}'")

# LLM 서버 호출
def query_llm(prompt: str) -> str:
    res = requests.post(config["llm_api_url"], json={
        "model": config["model_name"],
        "prompt": prompt,
        "max_tokens": config["max_tokens"],
        "temperature": config["temperature"],
        "stop": ["\n"]
    }, timeout=10)

    if res.status_code != 200:
        raise RuntimeError(f"LLM 응답 오류: {res.status_code}, {res.text}")
    
    result = res.json()
    return result.get("choices", [{}])[0].get("text", "").strip()

# 메인 루프
def run():
    print(f"🧠 LLM Agent 시작: topic={config['listen_topic']}", flush=True)
    for msg in consumer:
        try:
            data = msg.value
            prompt = build_prompt(data)
            decision = query_llm(prompt)
            print(f"✅ 전략 결정: {decision}", flush=True)

            producer.send(config["output_topic"], {
                "input": data,
                "decision": decision,
                "raw_prompt": prompt
            })
            producer.flush()

        except Exception as e:
            print(f"❌ 처리 실패: {e}", flush=True)

if __name__ == "__main__":
    run()
