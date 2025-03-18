FROM python:3.11

WORKDIR /app

# 의존성 파일 복사 및 설치
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# 애플리케이션 소스 복사
COPY backend /app/backend

# Python 경로 설정
ENV PYTHONPATH=/app/backend

# 애플리케이션 실행 명령
CMD ["uvicorn", "backend.main:app", "--host", "0.0.0.0", "--port", "8000"]