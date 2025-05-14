import os, yaml, json, torch, sys
import torch.nn as nn
import torch.optim as optim
from kafka import KafkaConsumer
sys.path.append(os.path.dirname(__file__))
from model import AEModel

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
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.model = AEModel(self.input_dim, self.sequence_length).to(self.device)
        self.optimizer = optim.AdamW(self.model.parameters(), lr=self.learning_rate)
        self.loss_fn = nn.MSELoss()
        self.batch = []

    def train_step(self):
        self.model.train()
        x = torch.tensor(self.batch, dtype=torch.float32).to(self.device)
        recon = self.model(x)
        loss = self.loss_fn(recon, x)
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()
        print(f"📉 Volatility AE Loss: {loss.item():.6f}")

    def export_onnx(self):
        self.model.eval()
        dummy_input = torch.randn(1, self.sequence_length, self.input_dim).to(self.device)
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        torch.onnx.export(
            self.model, dummy_input, self.model_path,
            input_names=["INPUT"], output_names=["OUTPUT"],
            dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
            opset_version=11
        )
        print(f"✅ ONNX Exported: {self.model_path}")

    def run(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="volatility_watcher_group"
        )
        print(f"🌪️ VolatilityWatcher consuming from: {self.topic}")

        for msg in consumer:
            value = msg.value
            features = value.get("input")
            if not features or len(features) != self.sequence_length:
                continue

            self.batch.append(features)
            if len(self.batch) >= self.batch_size:
                try:
                    self.train_step()
                    self.export_onnx()
                except Exception as e:
                    print(f"❌ Train error: {e}")
                finally:
                    self.batch.clear()
