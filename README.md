# Port

Backend(FastAPI 지원, WebSocket을 사용하며 /docs로 엔드포인트 확인 가능) : 8080
AI(Data-Mining) : 8081
AI(Pattern) : 8082
AI(Risk-Manage) : 8083
Kafka UI : 8088

# Env

Binance_Key :
Binance_Secret :

# 설치 및 실행법

## 설치

1. 환경설정을 위해서는 반드시 Docker가 필요하므로, 가장 먼저 설치해야 함.
2. .env 생성 및 세팅

## 실행

1. docker-compose --env-file .env up -d --build

# 하드웨어 사용환경

GPU를 사용하기 때문에 관련 하드웨어가 필수입니다. (현 개발환경 : NVIDIA RTX 4060 Ti/CUDA Version: 12.6)

# 사용방법

1. localhost:8080에서 엔드포인트 확인
