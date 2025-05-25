import os
import json
import yaml
import torch
import logging
from kafka import KafkaConsumer
from dotenv import load_dotenv

from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

load_dotenv()
mode = os.getenv("MODE", "prod").lower()
logger = logging.getLogger("LiquidityChecker")


class LiquidityCheckerAgent(BaseAgent):
    model_name_prefix = "liquidity_checker"
    model_class = TransformerAE

    def load_config(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.model_base_path = self.config["model_path"]
        self.batch_size = 1 if mode == "test" else self.config.get("batch_size", 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 5)
        self.d_model = self.config.get("d_model", 64)
        self.threshold = self.config.get("recon_error_threshold", 0.05)

    @property
    def model_path(self) -> str:
        return os.path.join(self.model_base_path, "stream", "model.pth")

    def save_model(self):
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        torch.save(self.model.state_dict(), self.model_path)
        logger.info(f"💾 모델 저장 완료: {self.model_path}")

    def load_model(self):
        if not os.path.exists(self.model_path):
            logger.warning(f"📂 모델 파일 없음: {self.model_path}")
            return
        self.model.load_state_dict(torch.load(self.model_path, map_location=self.device))
        self.model.eval()
        logger.info(f"📦 모델 로드 완료: {self.model_path}")

    def run_online(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="liquidity_checker_group"
        )
        logger.info(f"📡 LiquidityChecker consuming from: {self.topic}")

        for msg in consumer:
            value = msg.value
            symbol = value.get("symbol", "unknown")
            features = value.get("input")
            if not features or len(features) < self.sequence_length:
                continue
            features = features[-self.sequence_length:]
            self.batch.append(features)
            if len(self.batch) >= self.batch_size:
                try:
                    self.train_step()
                    recon_errors, flags = self.compute_recon_score(self.batch)
                    for err, flag in zip(recon_errors, flags):
                        logger.info(f"  🔎 Error: {err:.4f} | Liquidity anomaly: {'❌' if flag else '✅'}")
                    self.export_onnx(symbol=symbol)
                except Exception as e:
                    logger.error(f"❌ Train error: {e}")
                finally:
                    self.batch.clear()
