import os, sys, json, yaml, torch
import torch.nn as nn
import torch.optim as optim
from kafka import KafkaConsumer
from model import TransformerAE

sys.path.append(os.path.dirname(__file__))

class Agent:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.model_path = self.config["model_path"]
        self.batch_size = self.config.get("batch_size", 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 5)
        self.d_model = self.config.get("d_model", 64)
        self.threshold = self.config.get("recon_error_threshold", 0.05)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.model = TransformerAE(
            input_dim=self.input_dim,
            sequence_length=self.sequence_length,
            d_model=self.d_model
        ).to(self.device)

        self.optimizer = optim.AdamW(self.model.parameters(), lr=self.learning_rate)
        self.loss_fn = nn.MSELoss(reduction="none")
        self.batch = []

        print(f"🧠 PatternAE 초기화 완료 - Topic: {self.topic}", flush=True)

    def train_step(self):
        self.model.train()
        x = torch.tensor(self.batch, dtype=torch.float32).to(self.device)
        recon = self.model(x)
        loss = self.loss_fn(recon, x).mean()
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()
        print(f"📈 PatternAE Loss: {loss.item():.6f}", flush=True)

    def export_onnx(self):
        self.model.eval()
        dummy_input = torch.randn(1, self.sequence_length, self.input_dim).to(self.device)
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        torch.onnx.export(
            self.model, dummy_input, self.model_path,
            input_names=["INPUT"], output_names=["OUTPUT"],
            dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
            opset_version=13
        )
        print(f"✅ ONNX Exported: {self.model_path}", flush=True)

    def run(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="pattern_ae_group"
        )
        print(f"📊 PatternAE consuming from: {self.topic}", flush=True)

        for msg in consumer:
            features = msg.value.get("input")
            if not features or len(features) != self.sequence_length:
                continue

            self.batch.append(features)
            if len(self.batch) >= self.batch_size:
                try:
                    self.train_step()
                    self.export_onnx()
                except Exception as e:
                    print(f"❌ Train error: {e}", flush=True)
                finally:
                    self.batch.clear()
