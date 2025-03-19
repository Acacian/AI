from fastapi import APIRouter, WebSocket, WebSocketDisconnect
import threading
from core.socket import (
    start_binance_websocket
)

router = APIRouter()

@router.get("/start-websocket")
async def start_websocket():
    """WebSocket 실행 API - Binance WebSocket 시작"""
    thread = threading.Thread(target=start_binance_websocket, daemon=True)
    thread.start()
    return {"message": "WebSocket started"}

@router.websocket("/ws/binance")
async def websocket_binance(ws: WebSocket):
    """WebSocket 직접 연결"""
    await ws.accept()
    try:
        while True:
            data = await ws.receive_text()
            print(f"🔵 클라이언트로부터 수신: {data}")
            await ws.send_text(f"🔴 Binance WebSocket 연결됨: {data}")
    except WebSocketDisconnect:
        print("🚀 WebSocket 연결 종료됨")