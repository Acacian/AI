import os
import sys
import json
import yaml
import logging
import duckdb
import torch
import torch.nn as nn
import torch.optim as optim
import polars as pl
from kafka import KafkaConsumer
from dotenv import load_dotenv
from .model import TransformerAE

load_dotenv()

onnx_version = int(os.getenv("Onnx_Version", 17))
mode = os.getenv("MODE", "prod").lower()
DUCKDB_DIR = "duckdb"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | VolumeAE | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("VolumeAE")


class VolumeAEAgent:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.model_path = self.config["model_path"]
        self.batch_size = 1 if mode == "test" else self.config.get("batch_size", 32)
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

        logger.info(f"📦 Initialized - Topic: {self.topic}")

    def train_step(self):
        self.model.train()
        x = torch.tensor(self.batch, dtype=torch.float32).to(self.device)
        recon = self.model(x)
        loss = self.loss_fn(recon, x).mean()
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()
        logger.info(f"🔊 Loss: {loss.item():.6f}")

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
            self.model, dummy_input, self.model_path,
            input_names=["INPUT"], output_names=["OUTPUT"],
            dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
            opset_version=onnx_version
        )
        logger.info(f"✅ ONNX Exported: {self.model_path}")

    def should_pretrain(self):
        return not os.path.exists(self.model_path)

    def run_offline(self):
        logger.info("🦆 DuckDB 기반 오프라인 학습 시작")
        for db_file in sorted(os.listdir(DUCKDB_DIR)):
            if not db_file.endswith(".db"):
                continue
            db_path = os.path.join(DUCKDB_DIR, db_file)
            con = duckdb.connect(db_path)
            try:
                tables = con.execute("SHOW TABLES").fetchall()
                for (table_name,) in tables:
                    try:
                        df = con.execute(
                            f"SELECT open, high, low, close, volume FROM {table_name}"
                        ).fetchdf()
                        data = df.to_numpy()
                        if len(data) < self.sequence_length:
                            continue
                        for i in range(len(data) - self.sequence_length + 1):
                            seq = data[i:i + self.sequence_length].tolist()
                            self.batch.append(seq)
                            if len(self.batch) >= self.batch_size:
                                self.train_step()
                                self.batch.clear()
                    except Exception as e:
                        logger.warning(f"⚠️ Table {table_name} 처리 실패: {e}")
            finally:
                con.close()

        if self.batch:
            self.train_step()
            self.batch.clear()

        self.export_onnx()
        logger.info("✅ 오프라인 학습 완료")

    def run_online(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id=f"volume_ae_group_{os.getpid()}"
        )
        logger.info(f"📡 Kafka Listening - {self.topic}")

        for msg in consumer:
            value = msg.value
            x = value.get("input")

            if not x or len(x) != self.sequence_length:
                continue

            self.batch.append(x)
            if len(self.batch) >= self.batch_size:
                try:
                    self.train_step()
                    recon_errors, flags = self.compute_recon_score(self.batch)
                    for err, flag in zip(recon_errors, flags):
                        logger.info(f"  📊 Error: {err:.4f} | Volume anomaly: {'❌' if flag else '✅'}")
                    self.export_onnx()
                except Exception as e:
                    logger.error(f"❌ Train error: {e}")
                finally:
                    self.batch.clear()

    def run(self):
        if self.should_pretrain():
            logger.info("🧠 모델이 없어서 오프라인 학습 먼저 수행")
            self.run_offline()
        self.run_online()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("❌ 사용법: python -m agents_basket.volume_ae.agent <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    agent = VolumeAEAgent(config_path)
    agent.run()
