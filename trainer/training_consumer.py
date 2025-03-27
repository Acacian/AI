import os
import json
import torch
import torch.nn as nn
import torch.optim as optim
from kafka import KafkaConsumer

KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = "ai_training_btcusdt_1m"
MODEL_PATH = "/models/ai_mining/1/model.onnx"

# NVIDIA GPU 사용을 위한 장치 설정 (4060 Ti 기준)
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

class SimpleMiningModel(nn.Module):
    def __init__(self):
        super().__init__()
        self.fc = nn.Sequential(
            nn.Linear(5, 64),
            nn.ReLU(),
            nn.Linear(64, 64),
            nn.ReLU(),
            nn.Linear(64, 1)
        )

    def forward(self, x):
        return self.fc(x)

model = SimpleMiningModel().to(device)
optimizer = optim.AdamW(model.parameters(), lr=1e-3, weight_decay=1e-5)
loss_fn = nn.MSELoss()
batch = []


def train_step(batch_data):
    model.train()
    x = torch.tensor([d["input"] for d in batch_data], dtype=torch.float32).to(device)
    y = torch.tensor([d["target"] for d in batch_data], dtype=torch.float32).unsqueeze(1).to(device)

    optimizer.zero_grad()
    pred = model(x)
    loss = loss_fn(pred, y)
    loss.backward()
    optimizer.step()

    print(f"📈 loss = {loss.item():.6f}")


def export_onnx():
    model.eval()
    dummy_input = torch.randn(1, 5).to(device)
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    torch.onnx.export(
        model, dummy_input, MODEL_PATH,
        input_names=["INPUT"], output_names=["OUTPUT"],
        dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
        opset_version=11
    )
    print(f"✅ ONNX 저장: {MODEL_PATH}")


def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="trainer"
    )
    print(f"🎓 학습 시작: {TOPIC}")

    for msg in consumer:
        value = msg.value
        if not isinstance(value, dict) or "input" not in value or "target" not in value:
            continue

        batch.append(value)

        if len(batch) >= 32: 
            train_step(batch)
            export_onnx()
            batch.clear()


if __name__ == "__main__":
    main()
