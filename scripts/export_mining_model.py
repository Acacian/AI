import torch
import torch.nn as nn
import os
import time

class SimpleMiningModel(nn.Module):
    def __init__(self):
        super(SimpleMiningModel, self).__init__()
        self.fc = nn.Linear(1, 1)

    def forward(self, x):
        return self.fc(x)

if __name__ == "__main__":
    model = SimpleMiningModel()
    model.eval()

    input_sample = torch.randn(1, 1).float()
    model_path = "/models/ai_mining/1/model.onnx"
    os.makedirs(os.path.dirname(model_path), exist_ok=True)

    torch.onnx.export(
        model,
        input_sample,
        model_path,
        export_params=True,
        opset_version=11,
        do_constant_folding=True,
        input_names=["INPUT"],
        output_names=["OUTPUT"],
        dynamic_axes={"INPUT": {0: "batch"}, "OUTPUT": {0: "batch"}}
    )

    print(f"✅ 모델 저장 완료: {model_path}")

    # 컨테이너가 종료되지 않도록 유지하기 위한 루프
    try:
        while True:
            time.sleep(3600) 
    except KeyboardInterrupt:
        print("🛑 종료 요청 감지, 컨테이너 정리 중...")
