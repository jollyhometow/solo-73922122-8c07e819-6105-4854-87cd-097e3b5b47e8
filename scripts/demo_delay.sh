#!/usr/bin/env bash
# 演示：慢节点不挡查询、换件和另一分支。
# 给 validate_coords 设置 25 秒人为延迟后换件坐标CSV，
# 期间另一分支（到时校验）瞬时复用完成，状态查询保持毫秒级响应。
set -uo pipefail
BASE=${BASE_URL:-http://127.0.0.1:8000}
EX=$(cd "$(dirname "$0")/../examples" && pwd)

show() {
  curl -s "$BASE/api/status" | python3 -c "
import sys, json
d = json.load(sys.stdin)
r = d['run'] or {}
print(f\"  run#{r.get('id')} [{r.get('status')}]\")
for n in d['nodes']:
    print(f\"    {n['node']:<20} {n['status']:<8} 执行者={n['claimed_by'] or '-':<9} 复用={n['reused_from_run'] or '-'}\")"
}

echo "1) 设置 validate_coords 人为延迟 25 秒"
curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=validate_coords&seconds=25"

echo "2) 换件坐标CSV（生成唯一内容，触发新运行）"
NEWCOORDS=$(mktemp --suffix=.csv)
python3 -c "
import random
print('station,lat,lon')
for s, la, lo in [('BJI',40.018,116.168),('XAN',34.034,108.214),('LPA',29.657,91.132),('KMI',25.123,102.743)]:
    print(f'{s},{la + random.random()*0.0009:.6f},{lo}')
" > "$NEWCOORDS"
curl -s -o /dev/null -F "file=@$NEWCOORDS" "$BASE/upload/coords"

for i in 1 2 3 4; do
  T0=$(date +%s%N)
  echo "--- $((i*4))s 时的节点状态 ---"
  show
  T1=$(date +%s%N)
  echo "  （本次状态查询耗时 $(( (T1-T0)/1000000 )) ms —— 慢节点不挡查询）"
  sleep 4
done

echo "3) 慢节点运行期间换件到时CSV（不挡换件）"
curl -s -o /dev/null -F "file=@$EX/arrivals_v1.csv" "$BASE/upload/arrivals"
echo "   已受理。清除延迟，等待完成……"
curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=validate_coords&seconds=0"
for i in $(seq 1 30); do
  S=$(curl -s "$BASE/api/status" | python3 -c "import sys,json;print(json.load(sys.stdin)['run']['status'])")
  [ "$S" = "published" ] && break
  sleep 2
done
echo "4) 最终状态："
show
echo "   下载完整CSV: curl $BASE/download"
