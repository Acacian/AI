import os
import asyncio
import json
import signal
import websockets
import duckdb
import polars as pl
from datetime import datetime
from publisher import publish

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
DEPTH_LEVEL = 20
DUCKDB_DIR = "duckdb_orderbook"
BACKFILL_DAYS = int(os.getenv("Backfill_Days", 3))

is_running = True

# DuckDB 저장 함수
def save_orderbook_to_duckdb(symbol: str, payload: dict):
    os.makedirs(DUCKDB_DIR, exist_ok=True)
    today_str = datetime.now().strftime("%Y-%m-%d")
    db_path = os.path.join(DUCKDB_DIR, f"orderbook_{symbol}_{today_str}.db")
    table = f"orderbook_{symbol}"

    con = duckdb.connect(db_path)
    con.execute(f"CREATE TABLE IF NOT EXISTS {table} (timestamp BIGINT, bids TEXT, asks TEXT)")
    con.execute(f"INSERT INTO {table} VALUES (?, ?, ?)", [
        payload["timestamp"],
        json.dumps(payload["bids"]),
        json.dumps(payload["asks"])
    ])
    con.close()

# DuckDB 기반 오프라인 학습용 데이터 적재 함수 (초기화 시)
def load_orderbook_from_duckdb(symbol: str, sequence_length: int):
    db_files = sorted([f for f in os.listdir(DUCKDB_DIR) if f.startswith(f"orderbook_{symbol}_")])
    sequences = []
    for db_file in db_files:
        db_path = os.path.join(DUCKDB_DIR, db_file)
        con = duckdb.connect(db_path)
        try:
            df = con.execute(f"SELECT timestamp, bids, asks FROM orderbook_{symbol}").df()
            for i in range(len(df) - sequence_length + 1):
                window = df.iloc[i:i + sequence_length]
                seq = []
                for _, row in window.iterrows():
                    bids = json.loads(row["bids"])[:20]
                    asks = json.loads(row["asks"])[:20]
                    flat = [v for p in bids for v in p] + [v for p in asks for v in p]
                    seq.append(flat)
                sequences.append(seq)
        except Exception as e:
            print(f"❌ Failed to load {db_path}: {e}")
        finally:
            con.close()
    return sequences

# 실시간 웹소켓 처리
async def handle_symbol(symbol: str):
    global is_running
    url = f"{BINANCE_WS_URL}/{symbol}@depth{DEPTH_LEVEL}@100ms"
    while is_running:
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=10) as ws:
                print(f"🔌 [ORDERBOOK] Connected to {symbol}")
                while is_running:
                    try:
                        msg = await asyncio.wait_for(ws.recv(), timeout=15)
                        data = json.loads(msg)

                        payload = {
                            "symbol": symbol.upper(),
                            "timestamp": data["E"],
                            "bids": [[float(p), float(q)] for p, q in data.get("bids", [])],
                            "asks": [[float(p), float(q)] for p, q in data.get("asks", [])],
                        }

                        topic = f"orderbook_training_{symbol}"
                        publish(topic, payload)
                        save_orderbook_to_duckdb(symbol, payload)

                    except asyncio.TimeoutError:
                        print(f"⚠️ [ORDERBOOK] Timeout for {symbol}, retrying...")
                        break
                    except Exception as e:
                        print(f"❌ [ORDERBOOK] Error receiving for {symbol}: {e}")
                        break
        except Exception as e:
            print(f"🔁 [ORDERBOOK] Reconnecting {symbol} in 5s due to error: {e}")
            await asyncio.sleep(5)

# 종료 시그널 핸들러
def handle_sigterm():
    global is_running
    is_running = False
    print("🛑 [ORDERBOOK] Shutdown signal received.")

# 메인 루프
async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_sigterm)

    tasks = [asyncio.create_task(handle_symbol(symbol)) for symbol in SYMBOLS]
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
