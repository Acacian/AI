import websocket
import json

def on_message(ws, message):
    """Binance WebSocket에서 메시지를 받아 처리"""
    data = json.loads(message)
    price = float(data["p"])  # 현재 가격
    print(f"📈 BTC 가격: {price}")

def start_binance_websocket():
    """Binance WebSocket 실행 함수"""
    ws = websocket.WebSocketApp(
        "wss://stream.binance.com:9443/ws/btcusdt@trade",
        on_message=on_message
    )
    ws.run_forever(ping_interval=5)
