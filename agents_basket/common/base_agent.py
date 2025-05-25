from abc import ABC, abstractmethod
import os
import logging
import torch
import duckdb
import torch.nn as nn
import polars as pl

logger = logging.getLogger("BaseAgent")

class BaseAgent(ABC):
    model_name_prefix: str = "base_agent"
    model_class = None
    onnx_version: int = int(os.getenv("Onnx_Version", 17))

    def __init__(self, config_path: str):
        self.load_config(config_path)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.init_model()
        self.init_optimizer()
        self.batch = []

    def init_model(self):
        self.model = self.model_class(
            input_dim=self.input_dim,
            sequence_length=self.sequence_length,
            d_model=self.d_model
        ).to(self.device)

    def init_optimizer(self):
        self.optimizer = torch.optim.AdamW(self.model.parameters(), lr=self.learning_rate)
        self.loss_fn = nn.MSELoss(reduction="none")

    def train_step(self):
        self.model.train()
        x = torch.tensor(self.batch, dtype=torch.float32).to(self.device)
        recon = self.model(x)
        loss = self.loss_fn(recon, x).mean()
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()
        self.log(f"📈 Loss: {loss.item():.6f}")

    def compute_recon_score(self, x_batch):
        self.model.eval()
        with torch.no_grad():
            x = torch.tensor(x_batch, dtype=torch.float32).to(self.device)
            recon = self.model(x)
            error = (x - recon).pow(2).mean(dim=(1, 2))
            flags = (error > self.threshold).float()
        return error.tolist(), flags.tolist()

    def export_onnx(self, symbol: str, interval: str = "stream"):
        self.model.eval()
        dummy_input = torch.randn(1, self.sequence_length, self.input_dim).to(self.device)

        base_symbol = symbol.split("_")[0].lower()
        model_name = f"{self.model_name_prefix}_{base_symbol}"
        path = os.path.join(self.model_base_path, model_name, "1", "model.onnx")

        os.makedirs(os.path.dirname(path), exist_ok=True)
        try:
            torch.onnx.export(
                self.model, dummy_input, path,
                input_names=["INPUT"], output_names=["OUTPUT"],
                dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
                opset_version=self.onnx_version
            )
            self.log(f"✅ ONNX Exported: {path}")
        except Exception as e:
            self.log(f"❌ ONNX export 실패: {e}")

    def run_offline(self, duckdb_dir="duckdb"):
        for interval in self.config.get("intervals", ["1d"]):
            db_path = os.path.join(duckdb_dir, f"merged_{interval}.db")
            if not os.path.exists(db_path):
                self.log(f"⚠️ DB 없음: {db_path}")
                continue

            con = duckdb.connect(db_path, read_only=True)
            try:
                tables = con.execute("SHOW TABLES").fetchall()
                for (table_name,) in tables:
                    try:
                        df = con.execute(f'SELECT open, high, low, close, volume FROM "{table_name}"').df()
                        df = pl.DataFrame(df)
                        if df.shape[0] < self.sequence_length:
                            continue
                        data = df.to_numpy(dtype=float)
                        for i in range(len(data) - self.sequence_length + 1):
                            self.batch.append(data[i:i+self.sequence_length].tolist())
                            if len(self.batch) >= self.batch_size:
                                self.train_step()
                                self.batch.clear()
                        self.export_onnx(symbol=table_name, interval=interval)
                    except Exception as e:
                        self.log(f"⚠️ {table_name} 처리 실패: {e}")
            finally:
                con.close()

    def should_pretrain(self) -> bool:
        return not os.path.exists(self.model_path)

    def log(self, message: str):
        logger.info(f"[{self.__class__.__name__}] {message}")

    @abstractmethod
    def load_config(self, config_path): pass

    @abstractmethod
    def run_online(self): pass

    @abstractmethod
    def save_model(self): pass

    @abstractmethod
    def load_model(self): pass

    @property
    @abstractmethod
    def model_path(self) -> str: pass

    def run(self):
        if self.should_pretrain():
            self.log("🧠 모델 없음 → 오프라인 학습 시작")
            self.run_offline()
            self.save_model()
        else:
            self.log("📦 모델 로딩 시도")
            self.load_model()
        self.run_online()
