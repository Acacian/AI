import time
import json
import random
import requests
from datetime import datetime
from collections import deque
from gateway.kafka_producer import send_message

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"
SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
INTERVALS = ["1m", "5m", "15m", "1h", "1d"]
TRAIN_QUEUE = {symbol: {interval: deque(maxlen=3) for interval in INTERVALS} for symbol in SYMBOLS}

def label_strategy(data):
    if len(data) < 3:
        return 0
    close_now = data[-1]["close"]
    close_prev = data[-2]["close"]
    close_before = data[-3]["close"]
    pct_change_1 = (close_now - close_prev) / close_prev
    pct_change_2 = (close_prev - close_before) / close_before
    if pct_change_1 > 0.005 and pct_change_2 > 0.005:
        return 1
    elif pct_change_1 < -0.005 and pct_change_2 < -0.005:
        return 2
    return 0

def risk_label_strategy(data):
    if len(data) < 3:
        return 0
    high = max(d["high"] for d in data)
    low = min(d["low"] for d in data)
    close = data[-1]["close"]
    volatility = (high - low) / close

    if volatility > 0.01:
        return 1  
    return 0 

def format_ts(ts_ms):
    return datetime.utcfromtimestamp(ts_ms / 1000).strftime('%Y-%m-%d %H:%M:%S')

def fetch_kline(symbol, interval="1m", limit=1):
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    for attempt in range(3):
        try:
            response = requests.get(BINANCE_API_URL, params=params, timeout=3)
            if response.status_code != 200:
                time.sleep(1)
                continue
            data = response.json()
            if not data:
                return None
            d = data[0]
            return {
                "symbol": symbol,
                "timestamp": d[0],
                "open": float(d[1]),
                "high": float(d[2]),
                "low": float(d[3]),
                "close": float(d[4]),
                "volume": float(d[5]),
                "interval": interval
            }
        except Exception:
            time.sleep(2 ** attempt + random.random())
    return None

def backup_message(topic, message):
    try:
        with open(f"backup_{topic}.jsonl", "a") as f:
            f.write(json.dumps(message) + "\n")
    except Exception:
        pass

if __name__ == "__main__":
    last_sent = {}

    while True:
        start = time.time()
        failed_count = 0

        for symbol in SYMBOLS:
            for interval in INTERVALS:
                key = f"{symbol}-{interval}"
                kline = fetch_kline(symbol=symbol, interval=interval)
                if not kline:
                    failed_count += 1
                    continue

                ts = kline["timestamp"]
                close = kline["close"]
                prev = last_sent.get(key, {"timestamp": None, "close": None})
                if interval == "1m" or ts != prev["timestamp"] or close != prev["close"]:
                    try:
                        train_queue = TRAIN_QUEUE[symbol][interval]
                        train_queue.append(kline)
                        if len(train_queue) == 3:
                            pattern_target = label_strategy(list(train_queue))
                            risk_target = risk_label_strategy(list(train_queue))
                            features = [
                                kline["open"],
                                kline["high"],
                                kline["low"],
                                kline["close"],
                                kline["volume"]
                            ]

                            pattern_topic = f"ai_training_{symbol.lower()}_{interval}"
                            send_message(pattern_topic, {
                                "input": features,
                                "target": pattern_target
                            })

                            risk_topic = f"ai_risk_training_{symbol.lower()}_{interval}"
                            send_message(risk_topic, {
                                "input": features,
                                "target": risk_target
                            })

                            print(f"{symbol}-{interval} | {format_ts(ts)} | Close: {close} | Pattern: {pattern_target} | Risk: {risk_target}")

                        last_sent[key] = {"timestamp": ts, "close": close}
                    except Exception:
                        backup_message(pattern_topic, kline)
                        backup_message(risk_topic, kline)

        if failed_count >= 3:
            time.sleep(30)

        elapsed = time.time() - start
        time.sleep(max(0, 60 - elapsed))
