import torch
import torch.nn as nn
import torch.optim as optim
import os

# ✅ 리스크 평가 모델 정의
class RiskModel(nn.Module):
    def __init__(self):
        super(RiskModel, self).__init__()
        self.fc1 = nn.Linear(1, 16)
        self.fc2 = nn.Linear(16, 1)

    def forward(self, x):
        x = torch.relu(self.fc1(x))
        return self.fc2(x)

# ✅ 모델 학습 함수
def train_and_save_model():
    torch.manual_seed(42)
    model = RiskModel()
    criterion = nn.MSELoss()
    optimizer = optim.Adam(model.parameters(), lr=0.01)

    # ✅ 더미 데이터 생성 (리스크 계수 기반 예측)
    x_train = torch.rand((100, 1)) * 10
    y_train = x_train * 2 + torch.randn((100, 1))  # 단순한 선형 관계

    for epoch in range(200):
        optimizer.zero_grad()
        outputs = model(x_train)
        loss = criterion(outputs, y_train)
        loss.backward()
        optimizer.step()

        if epoch % 50 == 0:
            print(f"Epoch [{epoch}/200], Loss: {loss.item():.4f}")

    # ✅ 모델 저장
    os.makedirs("ai_risk_manage", exist_ok=True)
    torch.save(model.state_dict(), "ai_risk_manage/risk_model.pth")
    print("✅ 모델이 ai_risk_manage/risk_model.pth에 저장되었습니다.")

if __name__ == "__main__":
    train_and_save_model()
