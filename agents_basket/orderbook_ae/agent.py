import os, sys, json, yaml, logging, duckdb
from collections import deque
from datetime import datetime, timedelta
import torch
import torch.nn as nn
import torch.optim as optim
from kafka import KafkaConsumer, KafkaProducer
from dotenv import load_dotenv
from .model import TransformerAE
from agents_basket.common.base_agent import BaseAgent

load_dotenv()

onnx_version = int(os.getenv("Onnx_Version", 17))
mode = os.getenv("MODE", "prod").lower()
DUCKDB_DIR = "duckdb_orderbook"

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | OrderbookAgent | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("OrderbookAgent")


class OrderbookAgent(BaseAgent):
    def load_config(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.output_topic = self.config.get("output_topic", "orderbook_infer_input")
        self.model_base_path = self.config["model_path"]
        self.batch_size = 1 if mode == "test" else self.config.get("batch_size", 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 40)
        self.d_model = self.config.get("d_model", 64)
        self.threshold = self.config.get("recon_error_threshold", 0.05)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.batch = []
        self.sequence_buffer = deque(maxlen=self.sequence_length)
        self.producer = KafkaProducer(
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_serializer=lambda v: json.dumps(v).encode("utf-8")
        )

    def init_model(self):
        self.model = TransformerAE(
            input_dim=self.input_dim,
            sequence_length=self.sequence_length,
            d_model=self.d_model
        ).to(self.device)

    def init_optimizer(self):
        self.optimizer = optim.AdamW(self.model.parameters(), lr=self.learning_rate)
        self.loss_fn = nn.MSELoss(reduction="none")

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

    def flatten_orderbook(self, bids, asks):
        def flatten(side): return [float(v) for pair in side[:20] for v in pair]
        return flatten(bids) + flatten(asks)

    def train_step(self):
        self.model.train()
        x = torch.tensor(self.batch, dtype=torch.float32).to(self.device)
        recon = self.model(x)
        loss = self.loss_fn(recon, x).mean()
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()
        logger.info(f"🧠 Orderbook AE Loss: {loss.item():.6f}")

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
        logger.info(f"✅ ONNX exported: {self.model_path}")

    def run_offline(self):
        logger.info("🦆 DuckDB 기반 오프라인 학습 시작")
        for db_file in sorted(os.listdir(DUCKDB_DIR)):
            if not db_file.endswith(".db"):
                continue
            db_path = os.path.join(DUCKDB_DIR, db_file)
            con = duckdb.connect(db_path, read_only=True)
            try:
                tables = con.execute("SHOW TABLES").fetchall()
                for (table_name,) in tables:
                    try:
                        df = con.execute(f"SELECT bids, asks FROM {table_name}").fetchdf()
                        data = [self.flatten_orderbook(json.loads(b), json.loads(a))
                                for b, a in zip(df["bids"], df["asks"])]
                        for i in range(len(data) - self.sequence_length + 1):
                            seq = data[i:i+self.sequence_length]
                            if all(len(row) == self.input_dim for row in seq):
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
            group_id="orderbook_agent_group"
        )
        logger.info(f"📡 Kafka 수신 시작: {self.topic}")

        for msg in consumer:
            data = msg.value
            bids = data.get("bids")
            asks = data.get("asks")

            if not bids or not asks:
                continue

            flattened = self.flatten_orderbook(bids, asks)
            if len(flattened) != self.input_dim:
                continue

            self.sequence_buffer.append(flattened)

            if len(self.sequence_buffer) == self.sequence_length:
                self.batch.append(list(self.sequence_buffer))

                try:
                    self.train_step()
                    errors, flags = self.compute_recon_score(self.batch)
                    for err, flag in zip(errors, flags):
                        logger.info(f"  ⚠️ Recon Error: {err:.4f} | Anomaly: {'❌' if flag else '✅'}")

                    self.producer.send(self.output_topic, {"input": list(self.sequence_buffer)})
                    logger.info(f"📤 Triton 전송 완료 → {self.output_topic}")

                    self.export_onnx()
                except Exception as e:
                    logger.error(f"❌ 학습 중 오류: {e}")
                finally:
                    self.batch.clear()


if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("❌ 사용법: python -m agents_basket.orderbook.agent <config_path>")
        sys.exit(1)

    config_path = sys.argv[1]
    agent = OrderbookAgent(config_path)
    agent.run()
