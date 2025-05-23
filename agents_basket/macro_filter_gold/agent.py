import os, sys, json, yaml, logging, duckdb
import torch
import torch.nn as nn
import torch.optim as optim
import polars as pl
from kafka import KafkaConsumer
from .model import TransformerAE
from dotenv import load_dotenv

load_dotenv()

onnx_version = int(os.getenv("Onnx_Version", 17))
mode = os.getenv("MODE", "prod").lower()

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("MacroFilter")

class Agent:
    def __init__(self, config_path: str):
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)

        self.topic = self.config["topic"]
        self.model_base_path = self.config["model_path"]
        self.batch_size = 1 if mode == "test" else self.config.get('batch_size', 32)
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

    def train_step(self):
        self.model.train()
        x = torch.tensor(self.batch, dtype=torch.float32).to(self.device)
        recon = self.model(x)
        loss = self.loss_fn(recon, x).mean()
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()
        logger.info(f"📈 Macro AE Loss: {loss.item():.6f}")

    def compute_recon_score(self, x_batch):
        self.model.eval()
        with torch.no_grad():
            x = torch.tensor(x_batch, dtype=torch.float32).to(self.device)
            recon = self.model(x)
            error = (x - recon).pow(2).mean(dim=(1, 2))
            flags = (error > self.threshold).float()
        return error.tolist(), flags.tolist()

    def export_onnx(self, symbol: str, interval: str):
        self.model.eval()
        dummy_input = torch.randn(1, self.sequence_length, self.input_dim).to(self.device)
        path = os.path.join(self.model_base_path, interval, symbol, "model.onnx")
        os.makedirs(os.path.dirname(path), exist_ok=True)
        torch.onnx.export(
            self.model, dummy_input, path,
            input_names=["INPUT"], output_names=["OUTPUT"],
            dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
            opset_version=onnx_version
        )
        logger.info(f"✅ ONNX Exported: {path}")

    def run_offline(self, duckdb_dir="duckdb"):
        logger.info("🦆 DuckDB 기반 오프라인 학습 시작")
        intervals = self.config.get("intervals", ["1d"])
        for interval in intervals:
            db_path = os.path.join(duckdb_dir, f"merged_{interval}.db")
            if not os.path.exists(db_path):
                logger.warning(f"⚠️ DB 없음: {db_path} → 스킵됨")
                continue

            logger.info(f"📂 {interval} 학습 시작 ({db_path})")
            con = duckdb.connect(db_path)
            try:
                tables = con.execute("SHOW TABLES").fetchall()
                for (table_name,) in tables:
                    try:
                        df = con.execute(
                            f'SELECT open, high, low, close, volume FROM "{table_name}"'
                        ).df()
                        df = pl.DataFrame(df)
                        if df.shape[0] < self.sequence_length:
                            continue
                        data = df.to_numpy(dtype=float)
                        for i in range(len(data) - self.sequence_length + 1):
                            seq = data[i:i+self.sequence_length].tolist()
                            self.batch.append(seq)
                            if len(self.batch) >= self.batch_size:
                                self.train_step()
                                self.batch.clear()
                        self.export_onnx(symbol=table_name, interval=interval)
                    except Exception as e:
                        logger.warning(f"⚠️ {table_name} 처리 실패: {e}")
            finally:
                con.close()
        logger.info("✅ 오프라인 학습 완료")

    def should_pretrain(self):
        return True

    def run(self):
        if self.should_pretrain():
            logger.info("🧠 모델 파일이 없어 오프라인 학습을 먼저 수행합니다.")
            self.run_offline()

        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="macro_filter_group"
        )
        logger.info(f"📰 MacroFilter consuming from: {self.topic}")

        for msg in consumer:
            value = msg.value
            symbol = value.get("symbol", "unknown")
            interval = value.get("interval", "stream")
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
                        logger.info(f"  🔎 Error: {err:.4f} | Macro anomaly: {'❌' if flag else '✅'}")
                    self.export_onnx(symbol=symbol, interval=interval)
                except Exception as e:
                    logger.error(f"❌ Train error: {e}")
                finally:
                    self.batch.clear()

if __name__ == "__main__":
    if len(sys.argv) < 2:
        logger.error("❌ 사용법: python -m agents_basket.macro_filter.agent <config_path> [offline]")
        sys.exit(1)

    config_path = sys.argv[1]
    is_offline_mode = len(sys.argv) >= 3 and sys.argv[2].lower() == "offline"

    agent = Agent(config_path)

    if is_offline_mode:
        agent.run_offline()
        logger.info("🚀 초기 오프라인 학습만 실행 후 종료합니다.")
        sys.exit(0)
    else:
        agent.run()
