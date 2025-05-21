import os, sys, json, yaml, glob, torch
import torch.nn as nn
import torch.optim as optim
import polars as pl
from kafka import KafkaConsumer
from dotenv import load_dotenv
from .model import TransformerAE

load_dotenv()

onnx_version = int(os.getenv("Onnx_Version", 17))
mode = os.getenv("MODE", "prod").lower()

class PatternAEAgent:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.model_path = self.config["model_path"]
        self.batch_size = 1 if mode == "test" else self.config.get('batch_size', 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 5)
        self.d_model = self.config.get("d_model", 64)
        self.threshold = self.config.get("recon_error_threshold", 0.05)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = TransformerAE(self.input_dim, self.sequence_length, self.d_model).to(self.device)
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
            opset_version=onnx_version
        )
        print(f"✅ ONNX Exported: {self.model_path}", flush=True)

    def run_offline(self, data_dir="data"):
        print("📂 PatternAE 오프라인 학습 시작", flush=True)
        files = sorted(glob.glob(os.path.join(data_dir, "*/*.parquet")))
        for file_path in files:
            try:
                df = pl.read_parquet(file_path)
                if df.shape[0] < self.sequence_length:
                    continue
                data = df.select(["open", "high", "low", "close", "volume"]).to_numpy()
                for i in range(len(data) - self.sequence_length + 1):
                    self.batch.append(data[i:i+self.sequence_length].tolist())
                    if len(self.batch) >= self.batch_size:
                        self.train_step()
                        self.batch.clear()
            except Exception as e:
                print(f"⚠️ 파일 처리 실패: {file_path} | {e}", flush=True)

        if self.batch:
            self.train_step()
            self.batch.clear()

        self.export_onnx()
        print("✅ PatternAE 오프라인 학습 완료", flush=True)

    def should_pretrain(self):
        return not os.path.exists(self.model_path)

    def run(self):
        if self.should_pretrain():
            print("🔄 ONNX 모델 없음 → 오프라인 학습 수행", flush=True)
            self.run_offline()

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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("❌ 사용법: python -m agents_basket.pattern_ae.agent <config_path> [offline]")
        sys.exit(1)

    config_path = sys.argv[1]
    is_offline = len(sys.argv) >= 3 and sys.argv[2].lower() == "offline"

    agent = PatternAEAgent(config_path)

    if is_offline:
        agent.run_offline()
        print("🚀 오프라인 학습만 완료 후 종료", flush=True)
        sys.exit(0)

    agent.run()
