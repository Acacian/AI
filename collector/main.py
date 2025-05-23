import os, re, yaml, time, asyncio, signal
from datetime import datetime
from collector.data_collect.binance_rest import fetch_binance_symbol
from collector.data_collect.yahoo_rest import fetch_macro_symbol
from collector.scheduler import Scheduler
from collector.publisher import publish
from collector.data_processing.pre_processing import preprocess_ohlcv
from collector.database.backfill import backfill, SYMBOLS, INTERVALS, MACRO_SYMBOLS
from collector.database.duckdb import merge_parquet_dir
from collector.data_collect.orderbook_stream import main as orderbook_main

# Load config
with open("collector/config.yml") as f:
    config = yaml.safe_load(f)

binance_symbols = config.get("binance_symbols", [])
macro_symbols = config.get("macro_symbols", [])
binance_limit = int(os.getenv("Backfill_Binance_Limit", 1000))
yfinance_days = int(os.getenv("Backfill_Yfinance_Days", 90))
intervals = config.get("intervals", ["1m"])

interval_seconds = {
    "1m": 60,
    "5m": 300,
    "15m": 900,
    "30m": 1800,
    "1h": 3600,
    "1d": 86400,
}

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

scheduler = Scheduler()
shutdown_event = asyncio.Event()

def kafka_safe_symbol(symbol: str) -> str:
    return re.sub(r"[^a-zA-Z0-9._-]", "_", symbol.lower())

def should_send(interval: str, now: datetime) -> bool:
    sec = interval_seconds.get(interval)
    return sec and (int(now.timestamp()) % sec) < 3

async def run_ohlcv_loop():
    has_printed_topics = False
    while not shutdown_event.is_set():
        start = time.time()
        now = datetime.utcnow()

        for symbol in binance_symbols:
            for interval in intervals:
                if shutdown_event.is_set():
                    return
                if not should_send(interval, now):
                    continue

                print(f"📡 [BINANCE] Try fetch: {symbol}-{interval}")
                klines = fetch_binance_symbol(symbol, interval, binance_limit)
                if not klines or len(klines) < 3:
                    print(f"⚠️ No data or too short: {symbol}-{interval}")
                    continue

                ts = klines[-1]["timestamp"]
                if not scheduler.should_fetch(symbol, interval, ts):
                    print(f"🕒 Skip duplicate fetch: {symbol}-{interval} | ts={ts}")
                    continue

                raw = [[k["open"], k["high"], k["low"], k["close"], k["volume"]] for k in klines]
                features = preprocess_ohlcv(raw)

                safe_symbol = kafka_safe_symbol(symbol)
                for agent, topic_tpl in topics.items():
                    topic = topic_tpl.format(symbol=safe_symbol)
                    publish(topic, {
                        "symbol": symbol.lower(),
                        "interval": interval,
                        "input": features
                    })

                if not has_printed_topics:
                    print(f"📦 Published topics for {symbol}-{interval}:")
                    for topic in [topic_tpl.format(symbol=safe_symbol) for topic_tpl in topics.values()]:
                        print(f"  - {topic}")
                    has_printed_topics = True

                print(f"✅ [BINANCE] {symbol}-{interval} | Published to {len(topics)} agents")

        for symbol in macro_symbols:
            if shutdown_event.is_set():
                return

            print(f"📡 [MACRO] Try fetch: {symbol}")
            klines = fetch_macro_symbol(symbol, yfinance_days)
            if not klines or len(klines) < 3:
                print(f"⚠️ No data or too short: {symbol}")
                continue

            ts = klines[-1]["timestamp"]
            if not scheduler.should_fetch(symbol, "1d", ts):
                print(f"🕒 Skip duplicate fetch: {symbol} | ts={ts}")
                continue

            raw = [[k["open"], k["high"], k["low"], k["close"], k["volume"]] for k in klines]
            features = preprocess_ohlcv(raw)

            safe_symbol = kafka_safe_symbol(symbol)
            for agent, topic_tpl in topics.items():
                topic = topic_tpl.format(symbol=safe_symbol)
                publish(topic, {
                    "symbol": symbol.lower(),
                    "interval": "1d",
                    "input": features
                })

            print(f"✅ [MACRO] {symbol} | Published to {len(topics)} agents")

        elapsed = time.time() - start
        await asyncio.sleep(max(0, 60 - elapsed))

async def main():
    # Graceful shutdown 설정
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, shutdown_event.set)

    # 백필 + DuckDB 병합
    print("🔁 Backfill 시작...")
    for symbol in SYMBOLS:
        for interval in INTERVALS:
            if symbol in MACRO_SYMBOLS and interval != "1d":
                continue
            backfill(symbol, interval)

    print("🗃️ DuckDB 병합 시작...")
    for interval in INTERVALS:
        merge_parquet_dir(interval)

    with open("duckdb/.ready", "w") as f:
        f.write("collector_done")
    print("✅ 백필 + DuckDB 병합 완료: /app/duckdb/.ready 생성됨")

    # 실시간 수집 루프 병렬 실행
    try:
        await asyncio.gather(
            run_ohlcv_loop(),
            orderbook_main()
        )
    except asyncio.CancelledError:
        print("🛑 수집 루프가 취소되었습니다.")

if __name__ == "__main__":
    asyncio.run(main())
