import requests
import time
from gateway.kafka_producer import send_message

BINANCE_API_URL = "https://api.binance.com/api/v3/klines"

# ✅ 지원할 시간봉 목록
INTERVALS = {
    "1m": "ai_mining_1m",
    "5m": "ai_mining_5m",
    "15m": "ai_mining_15m",
    "1h": "ai_mining_1h",
    "1d": "ai_mining_1d"
}

def fetch_kline(symbol="BTCUSDT", interval="1m", limit=1):
    """ 바이낸스에서 특정 시간봉 데이터를 가져오는 함수 """
    params = {"symbol": symbol, "interval": interval, "limit": limit}
    response = requests.get(BINANCE_API_URL, params=params)
    
    if response.status_code != 200:
        print(f"⚠️ Binance API 요청 실패 ({interval}): {response.text}")
        return None
    
    data = response.json()[0]
    
    return {
        "timestamp": data[0],  # UNIX timestamp (밀리초)
        "open": float(data[1]),
        "high": float(data[2]),
        "low": float(data[3]),
        "close": float(data[4]),
        "volume": float(data[5]),
        "interval": interval  # ✅ 추가: 어느 시간봉인지 포함
    }

if __name__ == "__main__":
    last_sent = {interval: None for interval in INTERVALS}  # ✅ 각 시간봉 별 마지막 전송 시간 기록

    while True:
        for interval, topic in INTERVALS.items():
            kline = fetch_kline(interval=interval)
            
            if kline:
                # ✅ 1m 데이터는 항상 전송, 나머지는 마지막 값과 비교하여 중복 방지
                if interval == "1m" or kline["timestamp"] != last_sent[interval]:
                    print(f"📡 Binance 데이터 ({interval}): {kline}")
                    send_message(topic, kline)
                    last_sent[interval] = kline["timestamp"]  # 마지막 전송 기록 업데이트
        
        time.sleep(60)  # 1분마다 실행
