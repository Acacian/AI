import os, yaml, json, torch, sys
import torch.nn as nn
import torch.optim as optim
from kafka import KafkaConsumer
sys.path.append(os.path.dirname(__file__))
from model import TrendSegmenterLSTM

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
        self.hidden_size = self.config.get("hidden_size", 64)
        self.num_classes = self.config.get("num_classes", 3)
        self.device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

        self.model = TrendSegmenterLSTM(self.input_dim, self.hidden_size, self.num_classes).to(self.device)
        self.optimizer = optim.AdamW(self.model.parameters(), lr=self.learning_rate)
        self.loss_fn = nn.CrossEntropyLoss()
        self.batch_x, self.batch_y = [], []

        print(f"🧪 TrendSegmenter Agent 초기화 완료 - Topic: {self.topic}", flush=True)

    def train_step(self):
        print(f"🧪 Train Step 시작 - Batch size: {len(self.batch_x)}", flush=True)

        self.model.train()
        x = torch.tensor(self.batch_x, dtype=torch.float32).to(self.device)
        y = torch.tensor(self.batch_y, dtype=torch.long).to(self.device)

        logits = self.model(x)
        loss = self.loss_fn(logits, y)
        loss.backward()
        self.optimizer.step()
        self.optimizer.zero_grad()

        acc = (logits.argmax(1) == y).float().mean()
        print(f"📈 Trend Loss: {loss.item():.6f} | Acc: {acc.item():.4f}", flush=True)

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
        print(f"✅ ONNX Exported: {self.model_path}", flush=True)

    def run(self):
        consumer = KafkaConsumer(
            self.topic,
            bootstrap_servers=os.getenv("KAFKA_BROKER", "kafka:9092"),
            value_deserializer=lambda v: json.loads(v.decode("utf-8")),
            auto_offset_reset="latest",
            group_id="trend_segmenter_group"
        )
        print(f"📡 TrendSegmenter consuming from: {self.topic}", flush=True)

        for msg in consumer:
            value = msg.value
            x = value.get("input")
            y = value.get("target")
            print(f"📥 수신됨 - input len={len(x) if x else 'None'}, target={y}", flush=True)

            if not x or y is None or len(x) != self.sequence_length:
                print("⚠️ 무시됨 - 길이 부족 또는 None", flush=True)
                continue

            self.batch_x.append(x)
            self.batch_y.append(y)

            if len(self.batch_x) >= self.batch_size:
                try:
                    self.train_step()
                    self.export_onnx()
                except Exception as e:
                    print(f"❌ Train error: {e}", flush=True)
                finally:
                    self.batch_x.clear()
                    self.batch_y.clear()
