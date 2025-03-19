import uvicorn, asyncio
from dotenv import load_dotenv
from fastapi import FastAPI

# env load
load_dotenv(dotenv_path=".env")
app = FastAPI()

async def start_websocket():
    process = await asyncio.create_subprocess_exec("python", "backend/btc.py")
    await process.communicate() 

@app.on_event("startup")
async def startup_event():
    """FastAPI 서버 시작 시 WebSocket도 함께 실행"""
    asyncio.create_task(start_websocket())

if __name__ == "__main__":
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8000)
