import os
import json
import torch
import torch.nn as nn
import torch.optim as optim
from kafka import KafkaConsumer
from agents.pattern_ae.model_ae import AEModel

# 설정
KAFKA_BROKER = os.getenv("KAFKA_BROKER", "kafka:9092")
TOPIC = os.getenv("PATTERN_AE_TOPIC", "pattern_training_btcusdt_1m")
MODEL_PATH = os.getenv("MODEL_PATH", "/models/pattern_ae/1/model.onnx")

# 하이퍼파라미터 (나중에 config로 뺄 수 있음)
BATCH_SIZE = 32
LEARNING_RATE = 1e-3
WEIGHT_DECAY = 1e-5
SEQUENCE_LENGTH = 100
INPUT_DIM = 5

device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(f"🚀 Using device: {device}")

# 모델 및 최적화 세팅
model = AEModel(input_size=INPUT_DIM, seq_len=SEQUENCE_LENGTH).to(device)
optimizer = optim.AdamW(model.parameters(), lr=LEARNING_RATE, weight_decay=WEIGHT_DECAY)
loss_fn = nn.MSELoss()
batch = []

def train_step(batch_data):
    model.train()
    x = torch.tensor(batch_data, dtype=torch.float32).to(device)
    optimizer.zero_grad()
    recon = model(x)
    loss = loss_fn(recon, x)
    loss.backward()
    optimizer.step()
    print(f"📉 AE Loss: {loss.item():.6f}")

def export_onnx():
    model.eval()
    dummy_input = torch.randn(1, SEQUENCE_LENGTH, INPUT_DIM).to(device)
    os.makedirs(os.path.dirname(MODEL_PATH), exist_ok=True)
    torch.onnx.export(
        model, dummy_input, MODEL_PATH,
        input_names=["INPUT"], output_names=["OUTPUT"],
        dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}},
        opset_version=11
    )
    print(f"✅ ONNX Exported: {MODEL_PATH}")

def main():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=KAFKA_BROKER,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        auto_offset_reset="latest",
        group_id="trainer_pattern_ae"
    )
    print(f"🎓 AE Trainer Started: {TOPIC}")

    for msg in consumer:
        value = msg.value
        if not isinstance(value, dict):
            continue

        features = value.get("input")
        if not features or len(features) != SEQUENCE_LENGTH:
            continue

        batch.append(features)

        if len(batch) >= BATCH_SIZE:
            try:
                train_step(batch)
                export_onnx()
            except Exception as e:
                print(f"❌ Train error: {e}")
            finally:
                batch.clear()

if __name__ == "__main__":
    main()