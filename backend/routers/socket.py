from fastapi import APIRouter
import threading
from core.websocket_handler import (
    start_binance_websocket
)

router = APIRouter()

@router.get("/start-websocket")
async def start_websocket():
    """Binance WebSocket 실행 API"""
    thread = threading.Thread(target=start_binance_websocket, daemon=True)
    thread.start()
    return {"message": "WebSocket started"}
