import time
import random
import requests
import datetime

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

def fetch_binance_symbol(
    symbol: str,
    interval: str,
    limit: int = 1000,
    date_str: str = None,
    startTime: int = None,
    endTime: int = None
):
    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }

    if startTime:
        params["startTime"] = startTime
    if endTime:
        params["endTime"] = endTime

    # 일봉에 대해 미래 데이터 요청 방지
    if date_str and interval == "1d":
        today_str = datetime.datetime.utcnow().strftime("%Y-%m-%d")
        if date_str >= today_str:
            print(f"⚠️ {symbol}-{interval} | {date_str} 일봉은 Binance에서 아직 제공되지 않음 (skip)")
            return None

    for attempt in range(3):
        try:
            response = requests.get(BINANCE_API_URL, params=params, timeout=5)
            if response.status_code != 200:
                print(f"❌ Binance API 실패 [{response.status_code}]")
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
        except Exception as e:
            print(f"⚠️ Binance 예외 발생: {e}")
            time.sleep(2 ** attempt + random.random())
    return None
