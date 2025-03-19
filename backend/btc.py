import websocket
import json

def on_message(ws, message):
    data = json.loads(message)
    price = float(data["p"])  # 현재 가격
    print(f"📈 BTC 가격: {price}")

ws = websocket.WebSocketApp("wss://stream.binance.com:9443/ws/btcusdt@trade", on_message=on_message)
ws.run_forever(ping_interval=5)
