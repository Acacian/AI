# Multi AI AGENT Quent Manager

이 서버 프로그램은 독학으로 AI, 특히 Multi AI Agent를 학습하고 구현하며 기초를 다지고,  
백엔드와 연계하기 위해 시작한 프로젝트이며 수익을 우선으로 두고 있기는 하나, 항상 수익을  
보장하는 프로그램이 아니므로 투자에 대한 모든 책임은 투자한 본인에게 귀속됩니다.

## Settings

개발세팅 및 사용된 프레임워크
언어 : Python / FastApi
가상화 : Docker / Docker Compose
고속 데이터프레임 처리 : Polars (Pandas의 느린 처리속도를 대체)
멀티 AI 통신 : Redis, Kafka
추론 : triton(Agents에서 만든 ONNX 파일을 Polling 해서 사용)
학습 : torch

## Port

## Env

Binance_Key :
Binance_Secret :

## 실행법

1. docker-compose --env-file .env up -d --build

## 참고사항

1. GPU를 사용하기 때문에 최소한의 하드웨어가 필수입니다. (현 개발환경 : NVIDIA RTX 4060 Ti/CUDA Version: 12.6)
