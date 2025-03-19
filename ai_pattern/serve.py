import torch
import uvicorn
from fastapi import FastAPI
from pydantic import BaseModel

app = FastAPI()

class InputData(BaseModel):
    text: str

@app.post("/analyze")
def analyze(data: InputData):
    """ 패턴 분석 AI API """
    return {"pattern": "Detected pattern in text: " + data.text}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8082)
