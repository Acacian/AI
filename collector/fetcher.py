import time, random, requests, datetime

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

def fetch_kline(symbol: str, interval: str, limit: int = 1000, date_str: str = None):
    """
    지정한 날짜의 데이터를 정확히 수집하기 위해 startTime과 endTime을 모두 사용합니다.
    """
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    if date_str:
        start_dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        end_dt = start_dt + datetime.timedelta(days=1)

        from_ts = int(start_dt.timestamp() * 1000)
        to_ts = int(end_dt.timestamp() * 1000)

        params["startTime"] = from_ts
        params["endTime"] = to_ts

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
            print(f"⚠️ 예외 발생: {e}")
            time.sleep(2 ** attempt + random.random())
    return None
