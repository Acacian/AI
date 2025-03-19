import torch
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class InputData(BaseModel):
    features: list[float]

@app.post("/predict")
def predict(data: InputData):
    """ 데이터 마이닝 AI 모델 예측 API """
    return {"prediction": sum(data.features)}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8081)
