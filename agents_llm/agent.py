import os
import json
import yaml
import requests
from kafka import KafkaConsumer, KafkaProducer
from gateway.triton.triton_client import TritonClient

# 설정 로딩
with open("agents_llm/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# Triton client 초기화
triton = TritonClient()

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

# 프롬프트 생성 함수
def build_prompt(signal_data: dict) -> str:
    try:
        return config["prompt_template"].format(**signal_data)
    except KeyError as e:
        missing = str(e).strip("'")
        raise ValueError(f"프롬프트 생성 실패: 누락된 필드 '{missing}'")

# LLM 호출 함수
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
            if "input" not in data:
                print(f"⚠️ 'input' 필드 누락됨: {data}", flush=True)
                continue

            # signal feature 기반 추론
            models = ["pattern_ae", "risk_scorer", "volume_ae", "trend_segmenter", "volatility_watcher"]
            signal = triton.infer_signal(data, models)
            prompt = build_prompt(signal)
            decision = query_llm(prompt)
            print(f"✅ 전략 결정: {decision}", flush=True)

            producer.send(config["output_topic"], {
                "input": data,
                "signal": signal,
                "decision": decision,
                "raw_prompt": prompt
            })
            producer.flush()

        except Exception as e:
            print(f"❌ 처리 실패: {e}", flush=True)

if __name__ == "__main__":
    run()
