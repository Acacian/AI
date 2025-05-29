import os
import json
import torch
from dotenv import load_dotenv
from collections import deque
from kafka import KafkaProducer, KafkaConsumer
from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

load_dotenv()
mode = os.getenv("MODE", "prod").lower()

class OrderbookAgent(BaseAgent):
    model_name_prefix = "orderbook_ae"
    model_class = TransformerAE

    def load_config(self, config_path: str):
        import yaml
        with open(config_path, 'r') as f:
            full_config = yaml.safe_load(f)

        self.config = full_config[self.model_name_prefix]
        self.topic = self.config["topic"]
        self.output_topic = self.config.get("output_topic", "orderbook_infer_input")
        self.model_base_path = "/models"
        self.batch_size = 1 if mode == "test" else self.config.get("batch_size", 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 40)
        self.d_model = self.config.get("d_model", 64)
        self.threshold = self.config.get("recon_error_threshold", 0.05)

        self.sequence_buffer = deque(maxlen=self.sequence_length)
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def parse_features(self, value: dict) -> list[list[float]]:
        bids = value.get("bids")
        asks = value.get("asks")
        if not bids or not asks:
            return []

        def flatten(side): return [float(v) for pair in side[:20] for v in pair]
        flattened = flatten(bids) + flatten(asks)
        if len(flattened) != self.input_dim:
            return []

        self.sequence_buffer.append(flattened)
        if len(self.sequence_buffer) < self.sequence_length:
            return []
        return list(self.sequence_buffer)

    def run_online(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id=f"{self.model_name_prefix}_group"
        )
        self.log(f"📡 Subscribed to: {self.topic}")

        for msg in consumer:
            value = msg.value
            features = self.parse_features(value)
            if not features:
                continue

            self.batch.append(features)
            if len(self.batch) >= self.batch_size:
                try:
                    self.train_step()
                    errors, flags = self.compute_recon_score(self.batch)
                    for err, flag in zip(errors, flags):
                        self.log(f"⚠️ Recon Error: {err:.4f} | Anomaly: {'❌' if flag else '✅'}")

                    # Triton input 전송
                    self.producer.send(self.output_topic, {"input": features})
                    self.log(f"📤 Triton 전송 완료 → {self.output_topic}")

                    # ONNX 저장
                    self.export_onnx(symbol=value.get("symbol", "default"))
                except Exception as e:
                    self.log(f"❌ 학습 중 오류: {e}")
                finally:
                    self.batch.clear()
