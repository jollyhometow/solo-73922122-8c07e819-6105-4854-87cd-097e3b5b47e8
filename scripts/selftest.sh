#!/usr/bin/env bash
# 核心操作自测：校验→关联→发布→下载、缺坐标整次失败、重试、换件复用、
# 立即停供、迟到旧任务丢弃、行数上限。前置：服务已启动（docker compose 或 dev_local.sh）。
# 每次运行生成唯一测试数据，可重复执行。
set -uo pipefail

BASE=${BASE_URL:-http://127.0.0.1:8000}
EX=$(cd "$(dirname "$0")/../examples" && pwd)
TMP=$(mktemp -d)
PASS=0; FAIL=0

ok()  { echo "  ✔ $1"; PASS=$((PASS+1)); }
bad() { echo "  ✘ $1"; FAIL=$((FAIL+1)); }
eq()  { if [ "$2" = "$3" ]; then ok "$1"; else bad "$1（期望 [$3]，实得 [$2]）"; fi; }

jstatus() { curl -s "$BASE/api/status"; }
jrun()    { jstatus | python3 -c "import sys,json;d=json.load(sys.stdin);v=($1);print('' if v is None else v)"; }
run_status() { jrun "d['run']['status'] if d['run'] else 'none'"; }
run_id()     { jrun "d['run']['id'] if d['run'] else 0"; }
node_field() { jrun "next((n['$2'] for n in d['nodes'] if n['node']=='$1'), '')"; }

wait_status() { # 期望状态 超时秒
  local end=$((SECONDS + $2))
  while [ $SECONDS -lt $end ]; do
    [ "$(run_status)" = "$1" ] && return 0
    sleep 1
  done
  return 1
}

upload() { # kind file
  curl -s -o /dev/null -w '%{http_code}' -F "file=@$2" "$BASE/upload/$1"
}
dl_code() { curl -s -o "$TMP/dl.csv" -w '%{http_code}' "$BASE/download"; }

echo "== 0. 生成唯一测试数据，等待服务就绪 =="
python3 - "$EX/coords_v1.csv" "$TMP" <<'EOF'
import csv, random, sys
coords_path, tmp = sys.argv[1], sys.argv[2]
stations = ['BJI', 'XAN', 'LPA', 'KMI']
coords = {r['station']: r for r in csv.DictReader(open(coords_path))}
minute = random.randint(10, 50)  # 每次运行唯一的时刻，避免与历史内容指纹相同
for v in (1, 2, 3, 4):
    rows = [(s, f'2026-11-16T01:{minute:02d}:{v}{i}Z') for i, s in enumerate(stations)]
    with open(f'{tmp}/arr_v{v}.csv', 'w') as f:
        f.write('station,utc_time\n')
        for s, t in rows:
            f.write(f'{s},{t}\n')
    with open(f'{tmp}/expect_v{v}.csv', 'w') as f:
        f.write('station,utc_time,lat,lon\n')
        for s, t in rows:
            c = coords[s]
            f.write(f"{s},{t},{float(c['lat']):g},{float(c['lon']):g}\n")
EOF
for i in $(seq 1 60); do curl -sf "$BASE/api/status" >/dev/null 2>&1 && break; sleep 1; done
eq "服务就绪" "$(curl -s -o /dev/null -w '%{http_code}' "$BASE/api/status")" "200"

echo "== 1. 上传合法两份CSV → 校验+关联+发布 → 下载 =="
eq "上传到时CSV" "$(upload arrivals "$TMP/arr_v1.csv")" "303"
eq "上传坐标CSV" "$(upload coords "$EX/coords_v1.csv")" "303"
wait_status published 60 && ok "运行发布成功" || bad "运行发布超时"
eq "下载可用" "$(dl_code)" "200"
if diff -q "$TMP/expect_v1.csv" "$TMP/dl.csv" >/dev/null; then ok "下载内容正确"; else bad "下载内容不符"; diff "$TMP/expect_v1.csv" "$TMP/dl.csv"; fi

echo "== 2. 坐标缺台站 → 整次失败，不开放半份CSV =="
eq "上传缺台站坐标" "$(upload coords "$EX/coords_missing.csv")" "303"
wait_status failed 60 && ok "运行按预期失败" || bad "未转为失败"
eq "join节点失败" "$(node_field join status)" "failed"
jrun "next((n['error'] for n in d['nodes'] if n['node']=='join'), '')" | grep -q "缺少坐标" \
  && ok "错误信息指出缺坐标台站" || bad "缺少缺坐标错误信息"
eq "失败时下载被拒" "$(dl_code)" "409"

echo "== 3. 非法CSV（重复台站+坏时间）→ 校验失败 → 重试 =="
eq "上传非法到时CSV" "$(upload arrivals "$EX/invalid_arrivals.csv")" "303"
wait_status failed 60 || bad "未转为失败"
eq "validate_arrivals失败" "$(node_field validate_arrivals status)" "failed"
eq "坐标校验复用成功" "$(node_field validate_coords status)" "success"
ATT1=$(node_field validate_arrivals attempts)
curl -s -o /dev/null -X POST "$BASE/retry/validate_arrivals"
wait_status failed 60 || bad "重试后未再次失败"
eq "重试后尝试次数+1" "$(node_field validate_arrivals attempts)" "$((ATT1+1))"

echo "== 4. 换回合法两份CSV → 全部复用，直接恢复可下载 =="
eq "重新上传坐标v1" "$(upload coords "$EX/coords_v1.csv")" "303"
eq "重新上传到时v1" "$(upload arrivals "$TMP/arr_v1.csv")" "303"
wait_status published 60 && ok "恢复发布" || bad "恢复发布超时"
eq "下载恢复200" "$(dl_code)" "200"

echo "== 5. 换件到时CSV → 只重跑该分支及下游，坐标校验复用 =="
eq "上传到时v2" "$(upload arrivals "$TMP/arr_v2.csv")" "303"
wait_status published 60 && ok "换件后发布成功" || bad "换件后发布超时"
[ -n "$(node_field validate_coords reused_from_run)" ] && ok "坐标校验复用旧成功" || bad "坐标校验未复用"
[ -z "$(node_field validate_arrivals reused_from_run)" ] && ok "到时校验为重新执行" || bad "到时校验不应复用"
dl_code >/dev/null
if diff -q "$TMP/expect_v2.csv" "$TMP/dl.csv" >/dev/null; then ok "换件后下载为新内容"; else bad "换件后下载内容不符"; fi

echo "== 6. 换件瞬间旧结果立即停供 =="
curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=join&seconds=6"
upload arrivals "$TMP/arr_v3.csv" >/dev/null
eq "换件后旧结果立即停供(409)" "$(dl_code)" "409"
wait_status published 60 && ok "新运行发布成功" || bad "新运行发布超时"
eq "新结果可下载" "$(dl_code)" "200"
if diff -q "$TMP/expect_v3.csv" "$TMP/dl.csv" >/dev/null; then ok "下载内容为v3"; else bad "下载内容不是v3"; fi
curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=join&seconds=0"

echo "== 7. 迟到旧任务不得满足新依赖或覆盖结果 =="
curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=validate_arrivals&seconds=8"
upload arrivals "$TMP/arr_v4.csv" >/dev/null   # run N：校验被人为拖慢
sleep 2
upload arrivals "$TMP/arr_v1.csv" >/dev/null   # run N+1 取代 N（v1 全部复用，瞬时发布）
NEW_RUN=$(run_id)
eq "新运行瞬时发布(复用)" "$(run_status)" "published"
sleep 9                                        # 等 run N 的慢任务结束
eq "慢任务结束后当前运行未被顶掉" "$(run_id)" "$NEW_RUN"
eq "当前运行仍为已发布" "$(run_status)" "published"
dl_code >/dev/null
if diff -q "$TMP/expect_v1.csv" "$TMP/dl.csv" >/dev/null; then ok "迟到旧任务未覆盖结果"; else bad "结果被迟到任务覆盖"; fi
jrun "next((r['status'] for r in d['recent_runs'] if r['id']==$NEW_RUN-1), '')" | grep -q superseded \
  && ok "旧运行已标记superseded" || bad "旧运行未标记superseded"
curl -s -o /dev/null -X POST "$BASE/demo/delay" -d "node=validate_arrivals&seconds=0"

echo "== 8. 超过1000行上限 → 校验失败 =="
python3 -c "
print('station,utc_time')
for i in range(1001): print(f'STA{i:04d},2026-11-16T01:02:03Z')
" > "$TMP/big.csv"
upload arrivals "$TMP/big.csv" >/dev/null
wait_status failed 60 || bad "超限未失败"
jrun "next((n['error'] for n in d['nodes'] if n['node']=='validate_arrivals'), '')" | grep -q "超过上限" \
  && ok "报错含行数上限" || bad "缺少行数上限报错"
eq "失败时下载仍拒" "$(dl_code)" "409"
upload arrivals "$TMP/arr_v1.csv" >/dev/null   # 恢复可用终态
wait_status published 60 && ok "恢复发布" || bad "恢复发布超时"

echo "== 9. 执行进程在线 =="
ALIVE=$(jrun "sum(1 for w in d['workers'] if w['alive'])")
[ "$ALIVE" -ge 1 ] && ok "存活执行进程数=$ALIVE" || bad "无存活执行进程"

echo
echo "==================================="
echo "自测完成：通过 $PASS 项，失败 $FAIL 项"
[ $FAIL -eq 0 ]
