def pattern_label_strategy(data: list) -> int:
    close_now = data[-1]["close"]
    close_prev = data[-2]["close"]
    close_before = data[-3]["close"]
    pct_change_1 = (close_now - close_prev) / close_prev
    pct_change_2 = (close_prev - close_before) / close_before
    if pct_change_1 > 0.002 and pct_change_2 > 0.002:
        return 1
    elif pct_change_1 < -0.002 and pct_change_2 < -0.002:
        return 2
    return 0
