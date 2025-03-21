import torch
import torch.nn as nn
import os

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
