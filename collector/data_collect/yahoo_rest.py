import datetime
import yfinance as yf

def fetch_macro_symbol(symbol: str, limit: int = 100, date_str: str = None):
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
