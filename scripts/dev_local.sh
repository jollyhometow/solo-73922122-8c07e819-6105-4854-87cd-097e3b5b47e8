#!/usr/bin/env bash
# 无 Docker 环境下的等价启动：pgserver 提供 PostgreSQL，另起网页与两个执行进程。
# 有 Docker 时请直接使用 docker compose up。
set -euo pipefail
cd "$(dirname "$0")/.."

PGDATA_DIR=${PGDATA_DIR:-/tmp/seismic_pgdata}
DSN_FILE=${DSN_FILE:-/tmp/seismic_dsn}
STOP_FILE=${STOP_FILE:-/tmp/seismic_stop}
PORT=${PORT:-8000}

# 若上一轮进程仍在，先让其自行退出
if [ -f "$STOP_FILE" ]; then rm -f "$STOP_FILE"; fi

if [ ! -s "$DSN_FILE" ] || ! python3 -c "import psycopg2,sys; psycopg2.connect(open('$DSN_FILE').read()).close()" 2>/dev/null; then
  echo "启动 PostgreSQL (pgserver) ..."
  nohup python3 scripts/pgserver_hold.py "$PGDATA_DIR" "$DSN_FILE" "$STOP_FILE.pg" >/tmp/seismic_pg.log 2>&1 &
  for i in $(seq 1 90); do [ -s "$DSN_FILE" ] && break; sleep 1; done
fi

export DATABASE_URL=$(cat "$DSN_FILE")
export SELF_STOP_FILE="$STOP_FILE"
mkdir -p logs

echo "启动网页 (端口 $PORT) 与执行进程 worker1/worker2 ..."
nohup python3 -m uvicorn app.web:app --host 127.0.0.1 --port "$PORT" >logs/web.log 2>&1 &
nohup python3 -m app.worker worker1 >logs/worker1.log 2>&1 &
nohup python3 -m app.worker worker2 >logs/worker2.log 2>&1 &

for i in $(seq 1 60); do
  curl -sf "http://127.0.0.1:$PORT/api/status" >/dev/null 2>&1 && break
  sleep 1
done
echo "就绪: http://127.0.0.1:$PORT  (停止全部: touch $STOP_FILE)"
