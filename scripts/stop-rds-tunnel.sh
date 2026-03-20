#!/usr/bin/env bash
set -euo pipefail

LOCAL_PORT=3307

PID=$(lsof -t -i :"$LOCAL_PORT" -sTCP:LISTEN 2>/dev/null || true)

if [ -z "$PID" ]; then
    echo "[INFO] 没有发现运行中的隧道 (端口 $LOCAL_PORT)。"
    exit 0
fi

kill "$PID"
echo "[OK] 已关闭隧道进程 (PID: $PID, 端口: $LOCAL_PORT)。"
