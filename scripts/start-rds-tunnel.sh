#!/usr/bin/env bash
set -euo pipefail

LOCAL_PORT=3307
RDS_HOST="database-test.chomyssgs2p8.ap-southeast-1.rds.amazonaws.com"
RDS_PORT=3306
BASTION_USER="ec2-user"
BASTION_HOST="52.74.215.63"
SSH_KEY="$HOME/.ssh/id_rsa"

if lsof -i :"$LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "[OK] 隧道已在运行，本地端口 $LOCAL_PORT 已被监听。"
    exit 0
fi

echo "[INFO] 正在建立 SSH 隧道: 127.0.0.1:$LOCAL_PORT -> $RDS_HOST:$RDS_PORT (via $BASTION_HOST)"

ssh -f -o StrictHostKeyChecking=no \
    -o ServerAliveInterval=60 \
    -o ServerAliveCountMax=3 \
    -o ExitOnForwardFailure=yes \
    -N -L "$LOCAL_PORT:$RDS_HOST:$RDS_PORT" \
    -i "$SSH_KEY" \
    "$BASTION_USER@$BASTION_HOST"

sleep 1

if lsof -i :"$LOCAL_PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "[OK] 隧道建立成功，127.0.0.1:$LOCAL_PORT 已就绪。"
else
    echo "[ERROR] 隧道建立失败，请检查 SSH 密钥和跳板机连通性。"
    exit 1
fi
