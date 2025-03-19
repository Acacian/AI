# Env

Binance_Key :
Binance_Secret :

# 설치 및 실행법

## 설치

1. pip install -r requirements.txt 를 통해 전체 설치
2. pip install torch torchvision torchaudio --index-url https://download.pytorch.org/whl/cu126 를 통해 별도 torch 설치(GPU를 사용하기 때문에 별도의 설치가 필요함)

## 실행

1. .env 생성 및 세팅
2. docker-compose --env-file .env up -d --build

# 하드웨어 사용환경

GPU를 사용하기 때문에 관련 하드웨어가 필수입니다. (현 개발환경 : NVIDIA RTX 4060 Ti/CUDA Version: 12.6)
