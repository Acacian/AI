import os, json, datetime, yaml, re
from tqdm import tqdm
import polars as pl
from collector.data_collect.binance_rest import fetch_binance_symbol
from collector.data_collect.yahoo_rest import fetch_macro_symbol
from collector.data_processing.pre_processing import preprocess_ohlcv
from collector.publisher import publish

CONFIG_PATH = "collector/config.yml"
META_PATH = "data/last_ts.json"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

BINANCE_SYMBOLS = config.get("binance_symbols", [])
MACRO_SYMBOLS = config.get("macro_symbols", [])
SYMBOLS = BINANCE_SYMBOLS + MACRO_SYMBOLS

INTERVALS = config["intervals"]
LIMIT = int(os.getenv("Backfill_Binance_Limit",1000))
RETENTION_DAYS = int(os.getenv("Backfill_Days", 90))

TOPICS = {
    "liquidity_checker": "liquidity_training_{symbol}_{interval}",
    "trend_segmenter": "trend_training_{symbol}_{interval}",
    "noise_filter": "noise_training_{symbol}_{interval}",
    "risk_scorer": "risk_training_{symbol}_{interval}",
    "pattern": "pattern_training_{symbol}_{interval}",
    "volume_ae": "volume_training_{symbol}_{interval}",
    "macro_filter": "macro_training_{symbol}_{interval}",
    "overheat_detector": "overheat_training_{symbol}_{interval}",
    "volatility_watcher": "volatility_training_{symbol}_{interval}",
}

def kafka_safe_symbol(symbol: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", symbol.lower())

def get_paths(symbol: str, interval: str):
    data_dir = f"data/{interval}"
    return data_dir, META_PATH

def save_parquet(file_path: str, rows: list[dict]):
    if not rows:
        print(f"⚠️ 저장할 데이터 없음: {file_path}")
        return
    df = pl.DataFrame(rows)
    df.write_parquet(file_path)
    print(f"💾 저장 완료: {file_path} ({len(rows)} rows)")

def load_last_timestamp(meta_path: str, key: str):
    if not os.path.exists(meta_path):
        return None
    with open(meta_path, "r") as f:
        meta = json.load(f)
        return meta.get(key)

def save_last_timestamp(meta_path: str, key: str, ts: int):
    meta = {}
    if os.path.exists(meta_path):
        with open(meta_path, "r") as f:
            meta = json.load(f)
    meta[key] = ts
    with open(meta_path, "w") as f:
        json.dump(meta, f, indent=2)

def delete_old_files(data_dir: str, symbol: str):
    threshold = datetime.datetime.utcnow() - datetime.timedelta(days=RETENTION_DAYS)
    for fname in os.listdir(data_dir):
        if fname.startswith(symbol) and fname.endswith(".parquet"):
            try:
                date_str = fname.replace(f"{symbol}_", "").replace(".parquet", "")
                file_date = datetime.datetime.strptime(date_str, "%Y-%m-%d")
                if file_date < threshold:
                    os.remove(os.path.join(data_dir, fname))
                    print(f"🗑️ Deleted: {fname}")
            except Exception:
                continue

def fetch_full_day_klines(symbol, interval, limit, date_str):
    start_dt = datetime.datetime.strptime(date_str, "%Y-%m-%d")
    end_dt = start_dt + datetime.timedelta(days=1)

    from_ts = int(start_dt.timestamp() * 1000)
    to_ts = int(end_dt.timestamp() * 1000)

    # 인터벌별 1일당 데이터 수 기준 설정 (기준: 반복 호출 필요 여부 판단)
    INTERVAL_DAILY_COUNT = {
        "1m": 1440,
        "3m": 480,
        "5m": 288,
        "15m": 96,
        "30m": 48,
        "1h": 24,
        "2h": 12,
        "4h": 6,
        "6h": 4,
        "8h": 3,
        "12h": 2,
        "1d": 1
    }

    # 반복 호출이 불필요한 경우 (1일 데이터가 limit 이하인 경우)
    if INTERVAL_DAILY_COUNT.get(interval, 9999) <= limit:
        return fetch_binance_symbol(symbol, interval, limit, date_str)

    # 반복 호출이 필요한 경우 (기존 로직 유지)
    all_data = []
    while from_ts < to_ts:
        chunk = fetch_binance_symbol(symbol, interval, limit, date_str, startTime=from_ts)
        if not chunk:
            break

        all_data.extend(chunk)
        last_ts = chunk[-1]["timestamp"]
        from_ts = last_ts + 60_000  # 다음 분부터 시작

        if len(chunk) < limit:
            break

    return all_data

def safe_val(v):
    return float(v[0]) if isinstance(v, list) else float(v)

def backfill(symbol: str, interval: str):
    data_dir, meta_path = get_paths(symbol, interval)
    os.makedirs(data_dir, exist_ok=True)
    key = f"{symbol}_{interval}"

    start = datetime.datetime.utcnow() - datetime.timedelta(days=RETENTION_DAYS)
    end = datetime.datetime.utcnow() - datetime.timedelta(days=1)

    current = start
    with tqdm(total=(end - start).days + 1, desc=f"📦 {symbol}-{interval}") as pbar:
        while current <= end:
            date_str = current.strftime("%Y-%m-%d")
            file_path = os.path.join(data_dir, f"{symbol}_{date_str}.parquet")
            if os.path.exists(file_path):
                pbar.set_postfix_str(f"✅ Skip {date_str}")
            else:
                if symbol in MACRO_SYMBOLS:
                    klines = fetch_macro_symbol(symbol, LIMIT, date_str)
                else:
                    klines = fetch_full_day_klines(symbol, interval, LIMIT, date_str)

                if klines:
                    raw = [[safe_val(k["open"]), safe_val(k["high"]), safe_val(k["low"]), safe_val(k["close"]), safe_val(k["volume"])] for k in klines]
                    processed = preprocess_ohlcv(raw)

                    processed_rows = [
                        {
                            "timestamp": int(k["timestamp"]),
                            "symbol": str(k["symbol"]),
                            "interval": str(k["interval"]),
                            "open": float(row[0]),
                            "high": float(row[1]),
                            "low": float(row[2]),
                            "close": float(row[3]),
                            "volume": float(row[4]),
                        }
                        for k, row in zip(klines, processed)
                    ]

                    save_parquet(file_path, processed_rows)
                    save_last_timestamp(meta_path, key, klines[-1]["timestamp"])
                    pbar.set_postfix_str(f"📥 Saved {date_str}")
                else:
                    pbar.set_postfix_str(f"⚠️ No data {date_str}")
            current += datetime.timedelta(days=1)
            pbar.update(1)

    delete_old_files(data_dir, symbol)

if __name__ == "__main__":
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            if symbol in MACRO_SYMBOLS and interval != "1d":
                continue
            backfill(symbol, interval)
