import uvicorn
from fastapi import FastAPI
from gateway.kafka_consumer import consume_loop
import threading

app = FastAPI(
    title="AI Gateway",
    description="Kafka ↔ Triton 사이를 연결하는 FastAPI 게이트웨이 💫",
    version="1.0.0"
)

@app.get("/health")
def health_check():
    """🐣 헬스 체크 엔드포인트 (Docker용)"""
    return {"status": "✅ gateway alive", "detail": "Kafka & Triton 연결 준비 완료!"}

@app.on_event("startup")
def on_startup():
    """🌀 Kafka Consumer 쓰레드 부팅"""
    print("📡 Kafka Consumer 쓰레드 시작 중...")
    thread = threading.Thread(target=consume_loop, daemon=True)
    thread.start()

if __name__ == "__main__":
    uvicorn.run("gateway.main:app", host="0.0.0.0", port=8080, reload=True)
