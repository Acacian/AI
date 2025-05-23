import time, yaml, re, os
from datetime import datetime
from collector.data_collect.binance_rest import fetch_binance_symbol
from collector.data_collect.yahoo_rest import fetch_macro_symbol
from collector.scheduler import Scheduler
from collector.publisher import publish
from collector.data_processing.pre_processing import preprocess_ohlcv
from collector.database.backfill import backfill, SYMBOLS, INTERVALS, MACRO_SYMBOLS
from collector.database.duckdb import merge_parquet_dir

# Load configuration
with open("collector/config.yml") as f:
    config = yaml.safe_load(f)

binance_symbols = config.get("binance_symbols", [])
macro_symbols = config.get("macro_symbols", [])
binance_limit = int(os.getenv("Backfill_Binance_Limit", 1000))
yfinance_days = int(os.getenv("Backfill_Yfinance_Days", 90))
intervals = config.get("intervals", ["1m"])

scheduler = Scheduler()
topics = {
    "liquidity_checker": "liquidity_training_{symbol}",
    "trend_segmenter": "trend_training_{symbol}",
    "noise_filter": "noise_training_{symbol}",
    "risk_scorer": "risk_training_{symbol}",
    "pattern": "pattern_training_{symbol}",
    "volume_ae": "volume_training_{symbol}",
    "macro_filter": "macro_training_{symbol}",
    "overheat_detector": "overheat_training_{symbol}",
    "volatility_watcher": "volatility_training_{symbol}",
}

interval_seconds = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "1d": 86400,
}

has_printed_topics = False

def kafka_safe_symbol(symbol: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", symbol.lower())

def should_send(interval: str, now: datetime) -> bool:
    sec = interval_seconds.get(interval)
    if not sec:
        return False
    return (int(now.timestamp()) % sec) < 3  # 허용 오차 3초

# Backfill 단계
print("Backfill 시작...")
for symbol in SYMBOLS:
    for interval in INTERVALS:
        if symbol in MACRO_SYMBOLS and interval != "1d":
            continue
        backfill(symbol, interval)

print("DuckDB 병합 시작...")
for interval in INTERVALS:
    merge_parquet_dir(interval)

with open("duckdb/.ready", "w") as f:
    f.write("collector_done")
print("✅ 백필 + DuckDB 병합 완료: /app/duckdb/.ready 생성됨")

# 실시간 수집 단계
while True:
    start = time.time()
    now = datetime.utcnow()

    # Binance 심볼 수집
    for symbol in binance_symbols:
        for interval in intervals:
            if not should_send(interval, now):
                continue

            print(f"\U0001F4E1 [BINANCE] Try fetch: {symbol}-{interval}")
            klines = fetch_binance_symbol(symbol, interval, binance_limit)

            if not klines or len(klines) < 3:
                print(f"⚠️ No data or too short: {symbol}-{interval}")
                continue

            ts = klines[-1]["timestamp"]
            if not scheduler.should_fetch(symbol, interval, ts):
                print(f"🕒 Skip duplicate fetch: {symbol}-{interval} | ts={ts}")
                continue

            raw_features = [[k["open"], k["high"], k["low"], k["close"], k["volume"]] for k in klines]
            features = preprocess_ohlcv(raw_features)

            safe_symbol = kafka_safe_symbol(symbol)
            for agent, topic_tpl in topics.items():
                topic = topic_tpl.format(symbol=safe_symbol)
                publish(topic, {
                    "symbol": symbol.lower(),         # 예: "btcusdt"
                    "interval": interval,             # 예: "1m"
                    "input": features
                })

            if not has_printed_topics:
                print(f"\U0001F4E6 Published topics for {symbol}-{interval}:")
                for topic in [topic_tpl.format(symbol=safe_symbol) for topic_tpl in topics.values()]:
                    print(f"  - {topic}")
                has_printed_topics = True

            print(f"🟢 [BINANCE] {symbol}-{interval} | Published to {len(topics)} agents")

    # Macro 심볼 수집 (항상 1d)
    for symbol in macro_symbols:
        print(f"\U0001F4E1 [MACRO] Try fetch: {symbol}")
        klines = fetch_macro_symbol(symbol, yfinance_days)

        if not klines or len(klines) < 3:
            print(f"⚠️ No data or too short: {symbol}")
            continue

        ts = klines[-1]["timestamp"]
        if not scheduler.should_fetch(symbol, "1d", ts):
            print(f"🕒 Skip duplicate fetch: {symbol} | ts={ts}")
            continue

        raw_features = [[k["open"], k["high"], k["low"], k["close"], k["volume"]] for k in klines]
        features = preprocess_ohlcv(raw_features)

        safe_symbol = kafka_safe_symbol(symbol)
        for agent, topic_tpl in topics.items():
            topic = topic_tpl.format(symbol=safe_symbol)
            publish(topic, {
                "symbol": symbol.lower(),         # 예: "btcusdt"
                "interval": "1d",
                "input": features
            })

        print(f"🟢 [MACRO] {symbol} | Published to {len(topics)} agents")

    elapsed = time.time() - start
    time.sleep(max(0, 60 - elapsed))
