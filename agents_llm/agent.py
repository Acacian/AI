import json
import yaml
import time
import requests
import numpy as np
from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable
import tritonclient.http as httpclient

# TritonClient 클래스
class TritonClient:
    def __init__(self, url: str = "triton:8000"):
        self.client = httpclient.InferenceServerClient(url=url)

    def preprocess(self, data: dict) -> np.ndarray:
        features = data.get("input")
        if not isinstance(features, list):
            raise ValueError("❌ Triton 입력 형식이 올바르지 않습니다.")
        x = np.array(features, dtype=np.float32)
        if x.ndim == 2:
            x = np.expand_dims(x, axis=0)  # [1, seq_len, dim]
        return x

    def postprocess(self, output: np.ndarray) -> list:
        return output.tolist()

    def infer(self, model_name: str, data: dict) -> list:
        x = self.preprocess(data)
        inputs = [httpclient.InferInput("INPUT", x.shape, "FP32")]
        inputs[0].set_data_from_numpy(x)
        outputs = [httpclient.InferRequestedOutput("OUTPUT")]
        result = self.client.infer(model_name, inputs=inputs, outputs=outputs)
        return self.postprocess(result.as_numpy("OUTPUT"))

    def infer_signal(self, data: dict, models: list[str]) -> dict:
        signal = {}
        for model_name in models:
            try:
                output = self.infer(model_name, data)
                if isinstance(output, list) and len(output) == 1 and isinstance(output[0], list):
                    signal[model_name] = output[0]
                else:
                    signal[model_name] = output
            except Exception as e:
                print(f"❌ 모델 '{model_name}' 추론 실패: {e}")
                signal[model_name] = None
        return signal

# 설정 로딩
with open("agents_llm/config.yaml", "r") as f:
    config = yaml.safe_load(f)

# KafkaConsumer 연결
def init_consumer_with_retry(config: dict, max_retries: int = 10, delay_sec: int = 5):
    for attempt in range(max_retries):
        try:
            consumer = KafkaConsumer(
                config["listen_topic"],
                bootstrap_servers=config.get("kafka_broker", "kafka:9092"),
                group_id=config.get("group_id", "llm_agent_group"),
                value_deserializer=lambda m: json.loads(m.decode("utf-8")),
                auto_offset_reset="latest",
            )
            print("✅ Kafka 연결 성공")
            return consumer
        except NoBrokersAvailable:
            print(f"🔁 Kafka 연결 재시도 중... ({attempt+1}/{max_retries})")
            time.sleep(delay_sec)
    raise RuntimeError("❌ Kafka 연결 실패: No brokers available")

consumer = init_consumer_with_retry(config)

# KafkaProducer 생성
producer = KafkaProducer(
    bootstrap_servers=config.get("kafka_broker", "kafka:9092"),
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
    triton = TritonClient()

    for msg in consumer:
        try:
            data = msg.value
            if "input" not in data:
                print(f"⚠️ 'input' 필드 누락됨: {data}", flush=True)
                continue

            models = ["pattern_ae", "risk_scorer", "volume_ae", "trend_segmenter", "volatility_watcher"]
            signal = triton.infer_signal(data, models)
            prompt = build_prompt({
                "pattern": signal.get("pattern_ae"),
                "risk": signal.get("risk_scorer"),
                "volume": signal.get("volume_ae"),
                "trend": signal.get("trend_segmenter"),
                "volatility": signal.get("volatility_watcher"),
            })
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
