import datetime
import yfinance as yf

def fetch_macro_symbol(symbol: str, limit: int = 100):
    """
    최신 macro 데이터를 실시간으로 수집하기 위한 함수.
    오늘 날짜 기준으로 과거 limit 일 수만큼 데이터를 조회함.
    """
    end_dt = datetime.datetime.utcnow()
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
