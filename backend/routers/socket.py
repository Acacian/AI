from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import threading
from core.socket import (
    start_binance_websocket
)

router = APIRouter()

@router.get("/start-websocket-btc")
async def start_websocket():
    thread = threading.Thread(target=start_binance_websocket, daemon=True)
    thread.start()
    return {"message": "WebSocket started"}

@router.websocket("/ws/binance")
async def websocket_binance(ws: WebSocket):
    await ws.accept()
    try:
        while True:
            data = await ws.receive_text()
            print(f"🔵 클라이언트로부터 수신: {data}")
            await ws.send_text(f"🔴 Binance WebSocket 연결됨: {data}")
    except WebSocketDisconnect:
        print("🚀 WebSocket 연결 종료됨")