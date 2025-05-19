import os, json, datetime, yaml
from tqdm import tqdm
from collector.fetcher import fetch_kline
import polars as pl

CONFIG_PATH = "collector/config.yml"
META_PATH = "data/last_ts.json"

with open(CONFIG_PATH, "r") as f:
    config = yaml.safe_load(f)

SYMBOLS = config["symbols"]
INTERVALS = config["intervals"]
LIMIT = config.get("limit", 100)
RETENTION_DAYS = config.get("retention_days", 90)

def get_paths(symbol: str, interval: str):
    data_dir = f"data/{interval}"
    return data_dir, META_PATH

def save_parquet(file_path: str, klines: list[dict]):
    if not klines:
        print(f"⚠️ 저장할 데이터 없음: {file_path}")
        return
    df = pl.DataFrame(klines)
    df.write_parquet(file_path)
    print(f"💾 저장 완료: {file_path} ({len(klines)} rows)")

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
                klines = fetch_kline(symbol, interval, LIMIT, date_str)
                if klines:
                    save_parquet(file_path, klines)
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
            backfill(symbol, interval)
