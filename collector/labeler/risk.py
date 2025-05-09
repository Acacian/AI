def risk_label_strategy(data: list) -> int:
    high = max(d["high"] for d in data)
    low = min(d["low"] for d in data)
    close = data[-1]["close"]
    volatility = (high - low) / close
    if volatility > 0.005:
        return 1
    return 0
