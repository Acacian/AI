import uvicorn
from dotenv import load_dotenv
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi
from routers import socket

# env load
load_dotenv(dotenv_path=".env")
app = FastAPI()

app.include_router(socket.router, prefix="/socket", tags=["socket"])
def custom_openapi():
    """WebSocket 엔드포인트를 포함하여 OpenAPI 문서를 커스텀"""
    if app.openapi_schema:
        return app.openapi_schema

    openapi_schema = get_openapi(
        title="AI WebSocket Service",
        version="1.0.0",
        description="AI WebSocket 실시간 데이터 스트리밍 서비스",
        routes=app.routes,
    )

    # WebSocket 엔드포인트를 추가
    openapi_schema["paths"]["/socket/ws/binance"] = {
        "servers": [{"url": "ws://localhost:8000", "description": "WebSocket Server"}],
        "websocket": { 
            "summary": "Binance WebSocket 연결",
            "description": "Binance WebSocket에 연결하여 실시간 데이터를 수신",
            "operationId": "connectWebSocket",
            "responses": {
                "101": {"description": "WebSocket 연결 성공"},
                "400": {"description": "잘못된 요청"},
                "403": {"description": "접근 권한 없음"},
            },
        },
    }

    app.openapi_schema = openapi_schema
    return app.openapi_schema
app.openapi = custom_openapi

if __name__ == "__main__":
    uvicorn.run("backend.main:app", host="0.0.0.0", port=8080)
