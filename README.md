# 地震定位输入制备（到时CSV + 坐标CSV → 完整定位输入）

地震数据工程师更正原件后，通过网页换件即自动补跑定位输入制备，避免反演作业混用新旧数据。

## 架构

```
        ┌────────────┐
        │  网页 web   │ FastAPI：上传换件 / 节点状态 / 重试 / 下载 / 演示控制
        └─────┬──────┘
              │ 只读写数据库（慢节点不挡查询与换件）
        ┌─────▼──────┐
        │ PostgreSQL │ 全部状态：文件版本、运行、节点、租约、完整结果
        └─────▲──────┘
   认领(SKIP LOCKED)│租约心跳
   ┌──────┴───┐ ┌───┴──────┐
   │ worker1  │ │ worker2  │ 两个执行进程共享任务，互为接续
   └──────────┘ └──────────┘
```

流水线 DAG：`validate_arrivals`（校验到时CSV）与 `validate_coords`（校验坐标CSV）并行
→ `join`（按台站关联，缺坐标整次失败）→ `publish`（原子发布完整CSV）。

## 启动（Docker Compose）

```bash
docker compose up --build
# 网页: http://localhost:8000
```

compose 实际启动 4 个服务：`db`(PostgreSQL，数据卷持久化)、`web`、`worker1`、`worker2`。

无 Docker 的环境可用等价本地启动（pgserver 提供 PostgreSQL）：

```bash
bash scripts/dev_local.sh        # 停止: touch /tmp/seismic_stop
```

## 使用

1. 网页上传 **到时CSV**（`台站号,UTC到时`）与 **坐标CSV**（`台站号,纬度,经度`）。
   各至多 1000 数据行，台站号各自唯一；首行可为表头。两份分别校验。
2. 两份均通过后按台站号关联；任一到时台站缺坐标则整次失败，可查看错误并重试。
3. 成功后网页可下载含到时和坐标的完整 CSV（`station,utc_time,lat,lon`）。
4. **换件**：重新上传任一份即开新运行——只重跑该分支及下游节点，另一分支的
   成功校验按内容指纹直接复用；旧结果立即停供，下载在新运行发布前返回 409。

## 演示脚本

```bash
bash scripts/selftest.sh            # 核心操作自测（约1分钟）
bash scripts/demo_delay.sh          # 慢节点不挡查询/换件/另一分支
bash scripts/demo_exit.sh worker1   # worker1 崩溃，worker2 自动接续
# Docker 下也可手动：docker compose stop worker1
```

## 关键设计

| 需求 | 实现 |
|---|---|
| 两执行进程共享任务 | 任务即 `node_states` 行，`SELECT ... FOR UPDATE SKIP LOCKED` 认领 |
| 进程退出自动接续 | 15 秒租约 + 3 秒心跳；看门线程回收过期租约任务重新排队 |
| 换件只重跑受影响分支 | 节点输入指纹（文件 SHA256）；指纹相同的成功节点复用 |
| 立即停供旧结果 | 换件事务内将旧 run 置 `superseded`，下载只认最新 run 且须 `published` |
| 迟到旧任务不覆盖 | 提交结果时校验 run 仍为当前运行且租约有效，否则丢弃；依赖只在同 run 内满足 |
| 慢节点不挡查询/换件 | 网页只读数据库；执行进程每进程 4 线程并行 |
| 重启保留状态 | 全部状态在 PostgreSQL 数据卷；进程无状态 |
| 失败不出半份 CSV | `results` 行与 run 状态翻转同事务写入；失败时无 results 行 |

## 目录

```
app/pipeline.py   CSV 解析校验、关联、DAG 定义（纯函数）
app/worker.py     执行进程：认领/心跳/执行/提交/看门
app/web.py        FastAPI：页面、上传换件、重试、下载、演示控制
app/db.py         连接与建表
examples/         示例 CSV
scripts/          自测与演示脚本
```
