import time
import random
import requests

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

def fetch_kline(symbol: str, interval: str, limit: int = 100):
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
            return [
                {
                    "symbol": symbol,
                    "timestamp": d[0],
                    "open": float(d[1]),
                    "high": float(d[2]),
                    "low": float(d[3]),
                    "close": float(d[4]),
                    "volume": float(d[5]),
                    "interval": interval
                }
                for d in data
            ]
        except Exception:
            time.sleep(2 ** attempt + random.random())
    return None
