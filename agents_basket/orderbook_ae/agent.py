import os, sys
import json
import yaml
import torch
import torch.nn as nn
import torch.optim as optim
from kafka import KafkaConsumer
from .model import TransformerAE
from dotenv import load_dotenv

load_dotenv()

onnx_version = int(os.getenv("Onnx_Version", 17))
mode = os.getenv("MODE", "prod").lower()

class OrderbookAgent:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.model_path = self.config["model_path"]
        self.batch_size = 1 if mode == "test" else self.config.get('batch_size', 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 40)  # 20 bids + 20 asks
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

        print(f"🚀 OrderbookAgent initialized on topic: {self.topic}")

    def flatten_orderbook(self, bids, asks):
        """20개씩 잘라서 [p1, q1, p2, q2, ..., a1, q1, a2, q2, ...] 형태"""
        def flatten(side):
            return [v for pair in side[:20] for v in pair]
        return flatten(bids) + flatten(asks)

    def train_step(self):
        self.model.train()
        x = torch.tensor(self.batch, dtype=torch.float32).to(self.device)
        recon = self.model(x)
        loss = self.loss_fn(recon, x).mean()
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()
        print(f"🧠 Orderbook AE Loss: {loss.item():.6f}")

    def compute_recon_score(self, x_batch):
        self.model.eval()
        with torch.no_grad():
            x = torch.tensor(x_batch, dtype=torch.float32).to(self.device)
            recon = self.model(x)
            error = (x - recon).pow(2).mean(dim=(1, 2))
            flags = (error > self.threshold).float()
        return error.tolist(), flags.tolist()

    def export_onnx(self):
        self.model.eval()
        dummy_input = torch.randn(1, self.sequence_length, self.input_dim).to(self.device)
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        torch.onnx.export(
            self.model,
            dummy_input,
            self.model_path,
            input_names=["INPUT"],
            output_names=["OUTPUT"],
            dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
            opset_version=onnx_version
        )
        print(f"✅ ONNX exported: {self.model_path}")

    def run(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="orderbook_agent_group"
        )
        print(f"📥 Listening to Kafka topic: {self.topic}")

        for msg in consumer:
            data = msg.value
            bids = data.get("bids")
            asks = data.get("asks")

            if not bids or not asks:
                continue

            flattened = self.flatten_orderbook(bids, asks)
            if len(flattened) != self.input_dim:
                continue

            self.batch.append(flattened)

            if len(self.batch) >= self.batch_size:
                try:
                    self.train_step()
                    errors, flags = self.compute_recon_score(self.batch)
                    for err, flag in zip(errors, flags):
                        print(f"  ⚠️ Recon Error: {err:.4f} | Anomaly: {'❌' if flag else '✅'}")
                    self.export_onnx()
                except Exception as e:
                    print(f"❌ Training error: {e}")
                finally:
                    self.batch.clear()

if __name__ == "__main__":
    if len(sys.argv) != 2:
        print("❌ 사용법: python -m agents_basket.<agent_name>.agent <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    agent = OrderbookAgent(config_path)
    agent.run()