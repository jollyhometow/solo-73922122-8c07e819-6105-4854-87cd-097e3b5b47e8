#!/usr/bin/env bash
# 演示：执行进程退出由另一进程自动接续。
# 给 join 设置人为延迟，待 TARGET 进程认领后令其崩溃（或手动 docker compose stop worker1），
# 观察租约到期后另一进程回收任务并完成发布。
set -uo pipefail
BASE=${BASE_URL:-http://127.0.0.1:8000}
TARGET=${1:-worker1}
EX=$(cd "$(dirname "$0")/../examples" && pwd)

show() {
  curl -s "$BASE/api/status" | python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d['run'] or {}
ws = ' '.join(f\"{w['name']}={'存活' if w['alive'] else '退出'}\" for w in d['workers'])
print(f\"  run#{r.get('id')} [{r.get('status')}]  进程: {ws}\")
for n in d['nodes']:
    print(f\"    {n['node']:<20} {n['status']:<8} 执行者={n['claimed_by'] or '-':<9} 尝试={n['attempts']}\")"
}

# 生成一份全新到时CSV，保证 join 必须真跑（避免复用跳过演示）
NEWCSV=$(mktemp --suffix=.csv)
python3 -c "
import random
print('station,utc_time')
for s, t in [('BJI', 10), ('XAN', 20), ('LPA', 30), ('KMI', 40)]:
    print(f'{s},2026-11-16T02:03:{t + random.randint(0, 8):02d}Z')
" > "$NEWCSV"

echo "1) 设置 join 人为延迟 20 秒，并换件到时CSV"
curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=join&seconds=20"
curl -s -o /dev/null -F "file=@$NEWCSV" "$BASE/upload/arrivals"

echo "2) 等待 $TARGET 认领 join 任务 …"
for i in $(seq 1 20); do
  OWNER=$(curl -s "$BASE/api/status" | python3 -c "import sys,json;d=json.load(sys.stdin);print(next((n['claimed_by'] for n in d['nodes'] if n['node']=='join' and n['status']=='running'), ''))")
  [ -n "$OWNER" ] && break
  sleep 1
done
echo "   join 由 $OWNER 执行中（若与 $TARGET 不同属正常，任务由两进程共享争抢）"
OWNER=${OWNER:-$TARGET}

echo "3) 令 $OWNER 崩溃退出（模拟进程被杀）"
curl -s -o /dev/null -X POST "$BASE/demo/exit" -d "worker=$OWNER"
echo "   （Docker 部署时也可执行: docker compose stop $OWNER）"

echo "4) 观察另一进程接续（租约约15秒到期后回收）"
for i in $(seq 1 12); do
  sleep 5
  echo "--- $((i*5))s ---"
  show
  S=$(curl -s "$BASE/api/status" | python3 -c "import sys,json;print(json.load(sys.stdin)['run']['status'])")
  if [ "$S" = "published" ]; then
    echo "5) 已由另一进程接续完成并发布 ✔"
    curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=join&seconds=0"
    exit 0
  fi
done
curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=join&seconds=0"
echo "超时未发布，请检查执行进程日志" >&2
exit 1
