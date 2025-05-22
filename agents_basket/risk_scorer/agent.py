import os, sys, json, yaml, glob, logging
import torch
import torch.nn as nn
import torch.optim as optim
import polars as pl
from kafka import KafkaConsumer
from dotenv import load_dotenv
from .model import RiskScorerTransformer

load_dotenv()

onnx_version = int(os.getenv("Onnx_Version", 17))
mode = os.getenv("MODE", "prod").lower()

# 로깅 설정
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | RiskScorer | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("RiskScorer")

class RiskScorerAgent:
    def __init__(self, config_path):
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.model_path = self.config["model_path"]
        self.batch_size = 1 if mode == "test" else self.config.get("batch_size", 32)
        self.learning_rate = self.config.get("learning_rate", 1e-3)
        self.sequence_length = self.config.get("sequence_length", 100)
        self.input_dim = self.config.get("input_dim", 5)
        self.d_model = self.config.get("hidden_size", 64)
        self.num_classes = self.config.get("num_classes", 2)

        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.model = RiskScorerTransformer(
            input_dim=self.input_dim,
            sequence_length=self.sequence_length,
            d_model=self.d_model,
            num_classes=self.num_classes
        ).to(self.device)

        self.optimizer = optim.AdamW(self.model.parameters(), lr=self.learning_rate)
        self.loss_fn = nn.CrossEntropyLoss()
        self.batch_x, self.batch_y = [], []

        logger.info(f"⚠️ Initialized - Topic: {self.topic}")

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
        logger.info(f"📊 Train - Loss: {loss.item():.6f} | Accuracy: {acc.item():.4f}")

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
        logger.info(f"✅ ONNX model exported: {self.model_path}")

    def should_pretrain(self):
        return not os.path.exists(self.model_path)

    def run_offline(self, data_dir="data"):
        logger.info("📂 오프라인 학습 시작")
        files = sorted(glob.glob(os.path.join(data_dir, "*/*.parquet")))

        for file_path in files:
            try:
                df = pl.read_parquet(file_path)
                if df.shape[0] < self.sequence_length + 1 or "target" not in df.columns:
                    continue

                data = df.select(["open", "high", "low", "close", "volume"]).to_numpy()
                targets = df["target"].to_numpy()

                for i in range(len(data) - self.sequence_length):
                    x = data[i:i+self.sequence_length].tolist()
                    y = int(targets[i + self.sequence_length - 1])
                    self.batch_x.append(x)
                    self.batch_y.append(y)

                    if len(self.batch_x) >= self.batch_size:
                        self.train_step()
                        self.batch_x.clear()
                        self.batch_y.clear()

            except Exception as e:
                logger.warning(f"⚠️ 파일 처리 실패: {file_path} | {e}")

        if self.batch_x:
            self.train_step()
            self.batch_x.clear()
            self.batch_y.clear()

        self.export_onnx()
        logger.info("✅ 오프라인 학습 완료")

    def run(self):
        if self.should_pretrain():
            logger.info("🧠 ONNX 모델 없음 → 오프라인 학습 먼저 수행")
            self.run_offline()

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="risk_scorer_group"
        )
        logger.info(f"📡 Kafka consuming from: {self.topic}")

        for msg in consumer:
            value = msg.value
            x = value.get("input")
            y = value.get("target")

            if (
                not x or y is None
                or len(x) != self.sequence_length
                or not all(isinstance(row, list) and len(row) == self.input_dim for row in x)
            ):
                continue  # invalid shape

            self.batch_x.append(x)
            self.batch_y.append(y)

            if len(self.batch_x) >= self.batch_size:
                try:
                    self.train_step()
                    self.export_onnx()
                except Exception as e:
                    logger.error(f"❌ Train error: {e}")
                finally:
                    self.batch_x.clear()
                    self.batch_y.clear()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("❌ 사용법: python -m agents_basket.risk_scorer.agent <config_path> [offline]")
        sys.exit(1)

    config_path = sys.argv[1]
    is_offline = len(sys.argv) >= 3 and sys.argv[2].lower() == "offline"

    agent = RiskScorerAgent(config_path)

    if is_offline:
        agent.run_offline()
        logger.info("🏁 오프라인 학습 완료 후 종료")
        sys.exit(0)

    agent.run()
