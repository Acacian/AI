import asyncio
import json
import signal
import websockets
from publisher import publish

BINANCE_WS_URL = "wss://stream.binance.com:9443/ws"
SYMBOLS = ["btcusdt", "ethusdt", "solusdt"]
DEPTH_LEVEL = 20

is_running = True

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

                    except asyncio.TimeoutError:
                        print(f"⚠️ [ORDERBOOK] Timeout for {symbol}, retrying...")
                        break
                    except Exception as e:
                        print(f"❌ [ORDERBOOK] Error receiving for {symbol}: {e}")
                        break
        except Exception as e:
            print(f"🔁 [ORDERBOOK] Reconnecting {symbol} in 5s due to error: {e}")
            await asyncio.sleep(5)

def handle_sigterm():
    global is_running
    is_running = False
    print("🛑 [ORDERBOOK] Shutdown signal received.")

async def main():
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, handle_sigterm)

    tasks = [asyncio.create_task(handle_symbol(symbol)) for symbol in SYMBOLS]
    await asyncio.gather(*tasks, return_exceptions=True)

if __name__ == "__main__":
    asyncio.run(main())
