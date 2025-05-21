import uvicorn
from fastapi import FastAPI
from contextlib import asynccontextmanager
from triton_router import consume_loop
import threading

@asynccontextmanager
async def lifespan(app: FastAPI):
    try:
        thread = threading.Thread(target=consume_loop, daemon=True)
        thread.start()
        print("🚀 Triton Router Kafka Consumer 스레드 시작됨")
    except Exception as e:
        print(f"❌ Kafka Consumer 시작 실패: {e}")
    yield
    print("🛑 Triton Router 종료 중...")

app = FastAPI(lifespan=lifespan)

@app.get("/health")
def health_check():
    return {"status": "✅ Triton Router is running"}

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
