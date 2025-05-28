from abc import ABC, abstractmethod
import os, sys
import logging
import torch
import duckdb
import torch.nn as nn
import json
import time
import polars as pl
from .onetime_logger import one_time_logger
from kafka import KafkaConsumer
from .parse_features import get_parser_by_prefix

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(name)s | %(levelname)s | %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
    force=True 
)
logger = logging.getLogger("BaseAgent")
mode = os.getenv("MODE", "prod").lower()

class BaseAgent(ABC):
    model_name_prefix: str = "base_agent"
    model_class = None
    onnx_version: int = int(os.getenv("Onnx_Version", 17))

    def __init__(self, config_path: str):
        print(f"🟡 BaseAgent init 시작: {self.__class__.__name__}", flush=True)
        self.load_config(config_path)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
        self.init_model()
        self.init_optimizer()
        self.batch = []
        self.logger = one_time_logger

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

    def get_onnx_path(self, symbol: str) -> str:
        symbol = symbol.lower() 
        if "macro" in self.model_name_prefix:
            model_name = self.model_name_prefix
        else:
            model_name = f"{self.model_name_prefix}_{symbol}"
        return os.path.join(self.model_base_path, model_name, "1", "model.onnx")

    def export_onnx(self, symbol: str, interval: str = "stream"):
        self.model.eval()
        dummy_input = torch.randn(1, self.sequence_length, self.input_dim).to(self.device)

        path = self.get_onnx_path(symbol)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        self.log(f"📤 ONNX Export 시도 중... → {path}")

        try:
            torch.onnx.export(
                self.model, dummy_input, path,
                input_names=["INPUT"], output_names=["OUTPUT"],
                dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
                opset_version=self.onnx_version
            )
            self.log(f"✅ ONNX Exported: {path}")
            done_path = os.path.join(self.model_base_path, f"{self.model_name_prefix}.done")
            with open(done_path, "w") as f:
                f.write("done")
            self.log(f"🏁 .done 파일 생성 완료, triton 학습 시작 예정: {done_path}")
        except Exception as e:
            self.log(f"❌ ONNX export 실패: {e}")
            
    def run_offline(self, duckdb_dir="duckdb"):
        self.log("🟠 run_offline 진입")
        total_trained = 0
        total_skipped = 0
        total_tables = 0

        for interval in self.config.get("intervals", ["1d"]):
            db_path = os.path.join(duckdb_dir, f"merged_{interval}.db")
            if not os.path.exists(db_path):
                self.log(f"⚠️ DB 없음: {db_path}")
                continue

            con = duckdb.connect(db_path, read_only=True)
            try:
                tables = con.execute("SHOW TABLES").fetchall()
                for (table_name,) in tables:
                    total_tables += 1
                    try:
                        df = con.execute(f'SELECT open, high, low, close, volume FROM "{table_name}"').df()
                        df = pl.DataFrame(df)

                        if df.shape[0] < self.sequence_length:
                            total_skipped += 1
                            self.log(f"⏭️ {table_name} 스킵됨: 데이터 부족 ({df.shape[0]} rows)")
                            continue

                        data = df.to_numpy()
                        local_trained = 0

                        for i in range(len(data) - self.sequence_length + 1):
                            self.batch.append(data[i:i+self.sequence_length].tolist())
                            if len(self.batch) >= self.batch_size:
                                self.train_step()
                                total_trained += len(self.batch)
                                local_trained += len(self.batch)
                                self.batch.clear()

                        self.log(f"✅ {table_name} 학습 완료 | 샘플 수: {local_trained}")
                        self.export_onnx(symbol=table_name, interval=interval)

                    except Exception as e:
                        self.log(f"⚠️ {table_name} 처리 실패: {e}")
            finally:
                con.close()

        self.log(f"📊 Offline 학습 요약: 총 테이블 {total_tables}개 중 {total_trained}건 학습, {total_skipped}건 스킵됨")

    def run_online(self):
        time.sleep(1)
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id=f"{self.model_name_prefix}_group"
        )
        self.log(f"📡 Subscribed to: {self.topic}")

        for msg in consumer:
            value = msg.value
            symbol = value.get("symbol", "unknown")
            features = self.parse_features(value)
            if not features or len(features) < self.sequence_length:
                continue
            features = features[-self.sequence_length:]
            self.batch.append(features)
            if len(self.batch) >= self.batch_size:
                try:
                    self.train_step()
                    recon_errors, flags = self.compute_recon_score(self.batch)
                    for err, flag in zip(recon_errors, flags):
                        self.log(f"  🔎 Error: {err:.4f} | Anomaly: {'❌' if flag else '✅'}")
                    self.export_onnx(symbol=symbol)
                except Exception as e:
                    self.log(f"❌ Train error: {e}")
                finally:
                    self.batch.clear()

        done_path = os.path.join("models", f"{self.model_name_prefix}.done")
        try:
            with open(done_path, "w") as f:
                f.write("done")
            self.log(f"🏁 .done 파일 생성 완료: {done_path}")
        except Exception as e:
            self.log(f"❌ .done 파일 생성 실패: {e}")

    def parse_features(self, value: dict) -> list[list[float]]:
        parser = get_parser_by_prefix(self.model_name_prefix)
        return parser(value)

    def should_pretrain(self) -> bool:
        exists = os.path.exists(self.model_path)
        self.log(f"🧪 model_path 존재 여부: {self.model_path} → {'있음' if exists else '없음'}")
        return not exists

    def log(self, message: str, once: bool = False):
        if once:
            self.logger.info_once(f"[{self.__class__.__name__}] {message}")
        else:
            self.logger.info(f"[{self.__class__.__name__}] {message}")

    # ⚠️ num_classes, hidden_size 같은 분류 모델 전용 필드가 필요한 경우, 해당 agent에서 super().load_config() 호출 후 덧붙여 설정
    def load_config(self, config_path: str):
        import yaml

        with open(config_path) as f:
            full_config = yaml.safe_load(f)

        if self.model_name_prefix not in full_config:
            raise ValueError(f"❌ 설정 파일에 '{self.model_name_prefix}' 키가 없습니다")

        self.config = full_config[self.model_name_prefix]
        self.topic = self.config["topic"]
        self.model_base_path = self.config["model_path"]
        if mode == "test":
            self.batch_size = 1
            self.sequence_length = 1
            self.input_dim = 1
            self.d_model = 1
            self.learning_rate = 1e-4
            self.threshold = 0.5 
        else:
            self.batch_size = self.config.get("batch_size", 32)
            self.sequence_length = self.config.get("sequence_length", 100)
            self.input_dim = self.config.get("input_dim", 5)
            self.d_model = self.config.get("d_model", 64)
            self.learning_rate = self.config.get("learning_rate", 1e-3)
            self.threshold = self.config.get("recon_error_threshold", 0.05)

    def save_model(self):
        os.makedirs(os.path.dirname(self.model_path), exist_ok=True)
        torch.save(self.model.state_dict(), self.model_path)
        self.log(f"💾 모델 저장 완료: {self.model_path}")

    def load_model(self):
        if not os.path.exists(self.model_path):
            self.log(f"📂 모델 파일 없음: {self.model_path}")
            return
        self.model.load_state_dict(torch.load(self.model_path, map_location=self.device))
        self.model.eval()
        self.log(f"📦 모델 로드 완료: {self.model_path}")

    def extract_symbol(self) -> str:
        if "symbol" in self.config:
            return self.config["symbol"].lower()
        elif "_" in self.topic:
            return self.topic.split("_")[-1].lower()
        return "unknown"

    def export_onnx_if_needed(self):
        symbol = self.extract_symbol()
        try:
            self.export_onnx(symbol=symbol)
        except Exception as e:
            self.log(f"❌ ONNX Export 실패: {e}")

    @property
    def model_path(self) -> str:
        return os.path.join(self.model_base_path, f"{self.model_name_prefix}.pt")
    
    def run(self):
        if self.should_pretrain():
            self.log("🧠 모델 없음 → 오프라인 학습 시작")
            self.run_offline()
            self.save_model()
        else:
            self.log("📦 모델 로딩 시도")
            self.load_model()

        self.export_onnx_if_needed()
        self.run_online()


# =============================================================================================

class ClassificationBaseAgent(BaseAgent):
    def __init__(self, config_path: str):
        super().__init__(config_path)
        self.batch_x = []
        self.batch_y = []
        self.exported = False  # 중복 export 방지 (선택)

    def init_optimizer(self):
        self.optimizer = torch.optim.AdamW(self.model.parameters(), lr=self.learning_rate)
        self.loss_fn = torch.nn.CrossEntropyLoss()

    def init_model(self):
        self.model = self.model_class(
            input_dim=self.input_dim,
            sequence_length=self.sequence_length,
            hidden_size=self.d_model,
            num_classes=self.num_classes
        ).to(self.device)

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

        if not self.exported:
            self.export_onnx(symbol="default")
            self.exported = True

    def parse_features(self, value: dict) -> bool:
        x = value.get("input")
        y = value.get("target")

        if (
            not x or y is None
            or len(x) != self.sequence_length
            or not all(isinstance(row, list) and len(row) == self.input_dim for row in x)
        ):
            return False

        self.batch_x.append(x)
        self.batch_y.append(y)

        return len(self.batch_x) >= self.batch_size