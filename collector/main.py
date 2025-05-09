import time
import yaml
from fetcher import fetch_kline
from scheduler import Scheduler
from labeler.pattern import pattern_label_strategy
from labeler.risk import risk_label_strategy
from publisher import publish

with open("collector/config.yaml") as f:
    config = yaml.safe_load(f)

symbols = config["symbols"]
intervals = config["intervals"]
limit = config.get("limit", 100)
scheduler = Scheduler()

while True:
    start = time.time()
    for symbol in symbols:
        for interval in intervals:
            print(f"📡 Try fetch: {symbol}-{interval}", flush=True)
            klines = fetch_kline(symbol, interval, limit)
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

            # 패턴 라벨
            pattern_topic = f"ai_pattern_training_{symbol.lower()}_{interval}"
            pattern_target = pattern_label_strategy(klines)
            publish(pattern_topic, {
                "input": features,
                "target": pattern_target
            })

            # 리스크 라벨
            risk_topic = f"ai_risk_training_{symbol.lower()}_{interval}"
            risk_target = risk_label_strategy(klines)
            publish(risk_topic, {
                "input": features,
                "target": risk_target
            })

            print(f"🟢 {symbol}-{interval} | Pattern: {pattern_target} | Risk: {risk_target}")

    elapsed = time.time() - start
    time.sleep(max(0, 60 - elapsed))
