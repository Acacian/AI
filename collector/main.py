import time
import yaml
from datetime import datetime
from fetcher import fetch_kline
from scheduler import Scheduler
from publisher import publish

with open("collector/config.yaml") as f:
    config = yaml.safe_load(f)

symbols = config["symbols"]
intervals = config["intervals"]
limit = config.get("limit", 100)
scheduler = Scheduler()

# 테스트 또는 백필용 날짜
target_date_str = config.get("target_date")
target_date = datetime.strptime(target_date_str, "%Y-%m-%d") if target_date_str else None

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

while True:
    start = time.time()
    for symbol in symbols:
        for interval in intervals:
            print(f"📡 Try fetch: {symbol}-{interval}", flush=True)

            date_str = target_date.strftime("%Y-%m-%d") if target_date else None
            klines = fetch_kline(symbol, interval, limit, date_str=date_str)
            if not klines:
                print(f"❌ No data fetched for {symbol}-{interval}", flush=True)
                continue
            if len(klines) < 3:
                print(f"⚠️ Too short: {symbol}-{interval} | len={len(klines)}", flush=True)
                continue

            ts = klines[-1]["timestamp"]
            if not scheduler.should_fetch(symbol, interval, ts):
                print(f"🕒 Skip duplicate fetch: {symbol}-{interval} | ts={ts}", flush=True)
                continue

            features = [
                [k["open"], k["high"], k["low"], k["close"], k["volume"]]
                for k in klines
            ]

            for agent, topic_tpl in topics.items():
                topic = topic_tpl.format(symbol=symbol.lower(), interval=interval)
                publish(topic, {
                    "input": features
                })

            if not has_printed_topics:
                print(f"📦 Published topics for {symbol}-{interval}:")
                for topic in [topic_tpl.format(symbol=symbol.lower(), interval=interval) for topic_tpl in topics.values()]:
                    print(f"  - {topic}")
                has_printed_topics = True

            print(f"🟢 {symbol}-{interval} | Published to {len(topics)} agents")

    if target_date:
        print("✅ 날짜 기반 테스트 완료. 루프 종료.")
        break

    elapsed = time.time() - start
    time.sleep(max(0, 60 - elapsed))
