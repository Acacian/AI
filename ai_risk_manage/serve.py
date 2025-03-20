import torch
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

# FastAPI 앱 생성
app = FastAPI()

# ✅ 리스크 평가 AI 모델 정의
class RiskModel(torch.nn.Module):
    def __init__(self):
        super(RiskModel, self).__init__()
        self.fc1 = torch.nn.Linear(1, 16)
        self.fc2 = torch.nn.Linear(16, 1)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        return self.fc2(x)

# ✅ 모델 로드
device = "cuda" if torch.cuda.is_available() else "cpu"
model = RiskModel().to(device)

try:
    model.load_state_dict(torch.load("ai_risk_manage/risk_model.pth", map_location=device))
    model.eval()
    print("✅ 모델 로드 성공")
except FileNotFoundError:
    print("⚠️ 모델 파일을 찾을 수 없음. 기본 모델 사용.")

# ✅ 입력 데이터 구조 정의
class RiskData(BaseModel):
    risk_factor: float

@app.post("/risk-evaluate")
def evaluate_risk(data: RiskData):
    """ 리스크 관리 AI API """
    x = torch.tensor([[data.risk_factor]], dtype=torch.float32).to(device)

    with torch.no_grad():
        risk_score = model(x).cpu().item()

    return {"risk_score": risk_score}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)
