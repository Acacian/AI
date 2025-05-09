import time

class Scheduler:
    def __init__(self):
        self.last_ts = {}

    def should_fetch(self, symbol: str, interval: str, new_ts: int) -> bool:
        key = f"{symbol}-{interval}"
        if self.last_ts.get(key) == new_ts:
            return False
        self.last_ts[key] = new_ts
        return True
