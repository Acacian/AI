import uvicorn
from fastapi import FastAPI
from gateway.kafka_consumer import consume_loop
import threading

app = FastAPI()

@app.get("/health")
def health_check():
    return {"status": "✅ Gateway is running"}

@app.on_event("startup")
def start_kafka_consumer():
    thread = threading.Thread(target=consume_loop, daemon=True)
    thread.start()
    print("🚀 Kafka Consumer 스레드 시작됨")

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8080)
