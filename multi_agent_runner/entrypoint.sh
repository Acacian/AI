#!/bin/bash

READY_FILE="/app/duckdb/.ready"

echo "⏳ Collector 완료 대기 중: $READY_FILE"

while [ ! -f "$READY_FILE" ]; do
  echo "📁 아직 collector 완료 안 됨, 대기 중..."
  sleep 5
done

echo "✅ collector 완료됨. multi-agent-runner 시작."
exec python multi_agent_runner/main.py
