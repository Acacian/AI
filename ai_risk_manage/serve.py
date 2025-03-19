import torch
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class RiskData(BaseModel):
    risk_factor: float

@app.post("/risk-evaluate")
def evaluate_risk(data: RiskData):
    """ 리스크 관리 AI API """
    risk_score = data.risk_factor * 10
    return {"risk_score": risk_score}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8083)
