import time
import yaml
from fetcher import fetch_kline
from scheduler import Scheduler
from publisher import publish

with open("collector/config.yaml") as f:
    config = yaml.safe_load(f)

symbols = config["symbols"]
intervals = config["intervals"]
limit = config.get("limit", 100)
scheduler = Scheduler()

# 현재 사용하는 agent 목록과 대응하는 토픽 명명 규칙
topics = {
    "liquidity_checker": "liquidity_training_{symbol}_{interval}",
    "trend_segmenter": "trend_training_{symbol}_{interval}",
    "noise_filter": "noise_training_{symbol}_{interval}",
    "risk_scorer": "risk_training_{symbol}_{interval}",
    "pattern_ae": "pattern_ae_training_{symbol}_{interval}",
    "volume_ae": "volume_training_{symbol}_{interval}",
    "macro_filter": "macro_training_{symbol}_{interval}",
    "overheat_detector": "overheat_training_{symbol}_{interval}",
    "volatility_watcher": "volatility_training_{symbol}_{interval}",
}

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

            for agent, topic_tpl in topics.items():
                topic = topic_tpl.format(symbol=symbol.lower(), interval=interval)
                publish(topic, {
                    "input": features,
                    "target": 0  # AE 계열은 항상 0, LSTM은 나중에 조정
                })

            print(f"🟢 {symbol}-{interval} | All agent topics published")

    elapsed = time.time() - start
    time.sleep(max(0, 60 - elapsed))
