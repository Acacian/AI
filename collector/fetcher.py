import time, random, requests, datetime
import yfinance as yf

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

def fetch_kline(symbol: str, interval: str, limit: int = 1000, date_str: str = None):
    if symbol.startswith("^") or symbol.endswith("=F"):
        return fetch_macro_symbol(symbol, limit, date_str)
    return fetch_binance_symbol(symbol, interval, limit, date_str)

def fetch_binance_symbol(symbol: str, interval: str, limit: int, date_str: str = None):
    params = {"symbol": symbol, "interval": interval, "limit": limit}

    if date_str:
        start_dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        end_dt = start_dt + datetime.timedelta(days=1)
        params["startTime"] = int(start_dt.timestamp() * 1000)
        params["endTime"] = int(end_dt.timestamp() * 1000)

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

def fetch_macro_symbol(symbol: str, limit: int = 100, date_str: str = None):
    """
    Yahoo Finance에서 macro symbol 수집
    """
    if not date_str:
        print(f"❌ macro fetch는 target_date가 필요함")
        return None

    end_dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    start_dt = end_dt - datetime.timedelta(days=limit)

    try:
        df = yf.download(symbol, start=start_dt, end=end_dt, interval="1d")
        if df.empty or len(df) < 3:
            print(f"⚠️ Yahoo Finance 데이터 없음: {symbol}")
            return None
        return [
            {
                "symbol": symbol,
                "timestamp": int(ts.timestamp() * 1000),
                "open": row["Open"],
                "high": row["High"],
                "low": row["Low"],
                "close": row["Close"],
                "volume": row["Volume"],
                "interval": "1d"
            }
            for ts, row in df.iterrows()
        ]
    except Exception as e:
        print(f"⚠️ Yahoo 예외 발생: {e}")
        return None
