import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from gateway.kafka.kafka_consumer import consume_loop
import threading

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        thread = threading.Thread(target=consume_loop, daemon=True)
        thread.start()
        print("🚀 Kafka Consumer 스레드 시작됨")
    except Exception as e:
        print(f"❌ Kafka Consumer 시작 실패: {e}")

    yield  # 여기가 실행 중 구간

    print("🛑 Gateway shutting down...")

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health_check():
    return {"status": "✅ Gateway is running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
