import time, yaml, re
from datetime import datetime
from collector.data_collect.binance_rest import fetch_binance_symbol
from collector.data_collect.yahoo_rest import fetch_macro_symbol
from collector.scheduler import Scheduler
from collector.publisher import publish
from collector.data_processing.pre_processing import preprocess_ohlcv
from collector.database.backfill import backfill, SYMBOLS, INTERVALS, MACRO_SYMBOLS
from collector.database.duckdb import merge_parquet_dir

with open("collector/config.yml") as f:
    config = yaml.safe_load(f)

binance_symbols = config.get("binance_symbols", [])
macro_symbols = config.get("macro_symbols", [])
intervals = config["intervals"]
limit = config.get("limit", 100)
target_date_str = config.get("target_date")
target_date = datetime.strptime(target_date_str, "%Y-%m-%d") if target_date_str else None

scheduler = Scheduler()
topics = {
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

has_printed_topics = False

def kafka_safe_symbol(symbol: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", symbol.lower())


## Backfill / DuckDb Usage Stage
print("📦 Backfill 시작...")
for symbol in SYMBOLS:
    for interval in INTERVALS:
        if symbol in MACRO_SYMBOLS and interval != "1d":
            continue
        backfill(symbol, interval)

print("📁 DuckDB 병합 시작...")
for interval in INTERVALS:
    merge_parquet_dir(interval)


## Collecting Stage
while True:
    start = time.time()

    # Binance 심볼 루프
    for symbol in binance_symbols:
        for interval in intervals:
            print(f"📡 [BINANCE] Try fetch: {symbol}-{interval}")
            date_str = target_date.strftime("%Y-%m-%d") if target_date else None
            klines = fetch_binance_symbol(symbol, interval, limit, date_str)

            if not klines or len(klines) < 3:
                print(f"⚠️ No data or too short: {symbol}-{interval}")
                continue

            ts = klines[-1]["timestamp"]
            if not scheduler.should_fetch(symbol, interval, ts):
                print(f"🕒 Skip duplicate fetch: {symbol}-{interval} | ts={ts}")
                continue

            raw_features = [[k["open"], k["high"], k["low"], k["close"], k["volume"]] for k in klines]
            features = preprocess_ohlcv(raw_features) 

            for agent, topic_tpl in topics.items():
                topic = topic_tpl.format(symbol=symbol.lower(), interval=interval)
                publish(topic, {"input": features})

            if not has_printed_topics:
                print(f"📦 Published topics for {symbol}-{interval}:")
                for topic in [topic_tpl.format(symbol=symbol.lower(), interval=interval) for topic_tpl in topics.values()]:
                    print(f"  - {topic}")
                has_printed_topics = True

            print(f"🟢 [BINANCE] {symbol}-{interval} | Published to {len(topics)} agents")

    # Macro 심볼 루프
    for symbol in macro_symbols:
        interval = "1d"
        print(f"📡 [MACRO] Try fetch: {symbol}-{interval}")
        date_str = target_date.strftime("%Y-%m-%d") if target_date else None
        klines = fetch_macro_symbol(symbol, limit, date_str)

        if not klines or len(klines) < 3:
            print(f"⚠️ No data or too short: {symbol}-{interval}")
            continue

        ts = klines[-1]["timestamp"]
        if not scheduler.should_fetch(symbol, interval, ts):
            print(f"🕒 Skip duplicate fetch: {symbol}-{interval} | ts={ts}")
            continue

        raw_features = [[k["open"], k["high"], k["low"], k["close"], k["volume"]] for k in klines]
        features = preprocess_ohlcv(raw_features) 

        for agent, topic_tpl in topics.items():
            topic = topic_tpl.format(symbol=kafka_safe_symbol(symbol), interval=interval)
            publish(topic, {"input": features})

        print(f"🟢 [MACRO] {symbol}-{interval} | Published to {len(topics)} agents")

    if target_date:
        print("✅ 날짜 기반 테스트 완료. 루프 종료.")
        break

    elapsed = time.time() - start
    time.sleep(max(0, 60 - elapsed))

