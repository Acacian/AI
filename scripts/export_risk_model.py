import torch
import torch.nn as nn
import os

class RiskAssessmentModel(nn.Module):
    def __init__(self):
        super(RiskAssessmentModel, self).__init__()
        self.fc = nn.Linear(1, 1)

    def forward(self, x):
        return self.fc(x)

if __name__ == "__main__":
    model = RiskAssessmentModel()
    model.eval()

    input_sample = torch.randn(1, 1)
    model_path = "models/ai_risk_manage/1/model.onnx"
    
    os.makedirs(os.path.dirname(model_path), exist_ok=True)
    torch.onnx.export(model, input_sample, model_path, input_names=["INPUT"], output_names=["OUTPUT"])
    
    print(f"✅ 모델 저장 완료: {model_path}")
