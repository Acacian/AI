import datetime
import yfinance as yf

def fetch_macro_symbol(symbol: str, limit: int = 100, date_str: str = None):
    """
    매크로 심볼 데이터를 수집.
    - date_str이 주어지면 해당 날짜 하루치 데이터를 조회
    - 아니면 현재 시점 기준 과거 limit일 데이터를 조회
    """

    if date_str:
        start_dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
        end_dt = start_dt + datetime.timedelta(days=1)
    else:
        end_dt = datetime.datetime.utcnow()
        start_dt = end_dt - datetime.timedelta(days=limit)

    try:
        df = yf.download(symbol, start=start_dt, end=end_dt, interval="1d")
        if df.empty or len(df) < 1:
            print(f"⚠️ Yahoo Finance 데이터 없음: {symbol} | {date_str or f'최근 {limit}일'}")
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
        print(f"⚠️ Yahoo 예외 발생: {symbol} | {e}")
        return None
