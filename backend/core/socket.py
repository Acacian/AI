import websocket
import json
import threading
import pandas as pd
import requests

# ✅ 과거 데이터 가져오기
def get_historical_klines(symbol="BTCUSDT", interval="1m", limit=100):
    url = f"https://api.binance.com/api/v3/klines?symbol={symbol}&interval={interval}&limit={limit}"
    response = requests.get(url)
    data = response.json()
    df = pd.DataFrame(data, columns=["open_time", "open", "high", "low", "close", "volume",
                                     "close_time", "quote_asset_volume", "num_trades",
                                     "taker_buy_base", "taker_buy_quote", "ignore"])
    df = df[["open_time", "open", "high", "low", "close", "volume"]]
    df["open_time"] = pd.to_datetime(df["open_time"], unit="ms")
    df[["open", "high", "low", "close", "volume"]] = df[["open", "high", "low", "close", "volume"]].astype(float)
    return df

# ✅ 실시간 데이터 수집 (WebSocket)
def on_message(ws, message):
    data = json.loads(message)
    kline = data["k"]
    close_price = float(kline["c"])
    volume = float(kline["v"])
    interval = kline["i"]
    print(f"⏳ {interval} 캔들 - 종가: {close_price}, 거래량: {volume}")

def start_kline_websocket(interval="1m"):
    url = f"wss://stream.binance.com:9443/ws/btcusdt@kline_{interval}"
    ws = websocket.WebSocketApp(url, on_message=on_message)
    ws.run_forever(ping_interval=5)

# ✅ 과거 데이터 출력
df = get_historical_klines(limit=10)  # 최근 10개 가져오기
print(df)

# ✅ 실시간 데이터 수집 (1분 봉)
t = threading.Thread(target=start_kline_websocket, args=("1m",), daemon=True)
t.start()
t.join()
