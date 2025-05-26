import os
import yaml
import torch
from dotenv import load_dotenv
from .model import RiskScorerTransformer
from agents_basket.common.base_agent import BaseAgent

load_dotenv()
mode = os.getenv("MODE", "prod").lower()


class RiskScorerAgent(BaseAgent):
    model_name_prefix = "risk_scorer"
    model_class = RiskScorerTransformer

    def load_config(self, config_path):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.model_base_path = self.config["model_path"]
        self.batch_size = 1 if mode == "test" else self.config.get("batch_size", 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 5)
        self.d_model = self.config.get("hidden_size", 64)
        self.num_classes = self.config.get("num_classes", 2)

    @property
    def model_path(self) -> str:
        return os.path.join(self.model_base_path, "stream", "model.pth")

    def save_model(self):
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        torch.save(self.model.state_dict(), self.model_path)
        self.log(f"📎 모델 저장 완료: {self.model_path}")

    def load_model(self):
        if not os.path.exists(self.model_path):
            self.log(f"📂 모델 파일 없음: {self.model_path}")
            return
        self.model.load_state_dict(torch.load(self.model_path, map_location=self.device))
        self.model.eval()
        self.log(f"📦 모델 로드 완료: {self.model_path}")

    def init_model(self):
        self.model = self.model_class(
            input_dim=self.input_dim,
            sequence_length=self.sequence_length,
            d_model=self.d_model,
            num_classes=self.num_classes
        ).to(self.device)

    def init_optimizer(self):
        self.optimizer = torch.optim.AdamW(self.model.parameters(), lr=self.learning_rate)
        self.loss_fn = torch.nn.CrossEntropyLoss()

    def train_step(self):
        self.model.train()
        x = torch.tensor(self.batch_x, dtype=torch.float32).to(self.device)
        y = torch.tensor(self.batch_y, dtype=torch.long).to(self.device)
        logits = self.model(x)
        loss = self.loss_fn(logits, y)
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()
        acc = (logits.argmax(1) == y).float().mean()
        self.log(f"📊 Train - Loss: {loss.item():.6f} | Accuracy: {acc.item():.4f}")

    def run_online(self):
        from kafka import KafkaConsumer
        import json
        import os

        self.batch_x, self.batch_y = [], []
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id=f"{self.model_name_prefix}_group_{os.getpid()}"
        )

        self.log(f"📱 Kafka consuming from: {self.topic}")

        for msg in consumer:
            value = msg.value
            x = value.get("input")
            y = value.get("target")

            if (
                not x or y is None
                or len(x) != self.sequence_length
                or not all(isinstance(row, list) and len(row) == self.input_dim for row in x)
            ):
                continue

            self.batch_x.append(x)
            self.batch_y.append(y)

            if len(self.batch_x) >= self.batch_size:
                try:
                    self.train_step()
                    self.save_model()
                    self.export_onnx(symbol=value.get("symbol", "default"))
                except Exception as e:
                    self.log(f"❌ Train error: {e}")
                finally:
                    self.batch_x.clear()
                    self.batch_y.clear()
