import os
import json

class Scheduler:
    def __init__(self, cache_file: str = None):
        self.last_ts = {}
        self.cache_file = cache_file
        if self.cache_file and os.path.exists(self.cache_file):
            self._load_cache()

    def should_fetch(self, symbol: str, interval: str, new_ts: int) -> bool:
        key = f"{symbol}-{interval}"
        if self.last_ts.get(key) == new_ts:
            return False
        self.last_ts[key] = new_ts
        self._save_cache()
        return True

    def _load_cache(self):
        try:
            with open(self.cache_file, "r") as f:
                self.last_ts = json.load(f)
            print(f"🧠 캐시 로드 완료: {self.cache_file}")
        except Exception as e:
            print(f"⚠️ 캐시 로드 실패: {e}")

    def _save_cache(self):
        if not self.cache_file:
            return
        try:
            with open(self.cache_file, "w") as f:
                json.dump(self.last_ts, f)
        except Exception as e:
            print(f"⚠️ 캐시 저장 실패: {e}")
