"""执行进程（executor）。两个实例共享 PostgreSQL 中的任务队列：

- 认领：SELECT ... FOR UPDATE SKIP LOCKED，互不抢同一任务；
- 租约：认领后周期性心跳续租，进程崩溃租约到期由另一进程回收重跑；
- 迟到保护：提交结果时校验所属 run 仍是当前运行，被替换的旧运行结果直接丢弃，
  不会满足新运行的依赖，也不会覆盖新结果。

用法: python -m app.worker <worker名>
"""
import logging
import os
import signal
import sys
import threading
import time

from psycopg2.extras import Json

from . import db, pipeline

LEASE_SECONDS = 15          # 租约时长；进程崩溃后任务最多滞留这么久即被回收
HEARTBEAT_SECONDS = 3       # 心跳续租间隔
POLL_SECONDS = 1.0          # 无任务时的轮询间隔
EXEC_THREADS = 4            # 单进程并发执行任务数（慢节点不占用全部执行能力）
JANITOR_SECONDS = 2.0

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)

stop_event = threading.Event()


class Worker:
    def __init__(self, name: str):
        self.name = name
        self.log = logging.getLogger(name)

    # ---------- 认领任务 ----------
    def claim_one(self):
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    """
                    UPDATE node_states ns
                       SET status='running', claimed_by=%s,
                           lease_expires_at=now()+(%s||' seconds')::interval,
                           attempts=ns.attempts+1, updated_at=now()
                     WHERE (ns.run_id, ns.node) = (
                           SELECT ns2.run_id, ns2.node
                             FROM node_states ns2
                             JOIN runs r ON r.id=ns2.run_id AND r.status='running'
                            WHERE ns2.status='pending'
                              AND NOT EXISTS (
                                    SELECT 1 FROM node_deps d
                                    JOIN node_states dep
                                      ON dep.run_id=ns2.run_id
                                     AND dep.node=d.depends_on
                                     AND dep.status<>'success'
                                    WHERE d.node=ns2.node)
                            ORDER BY ns2.run_id DESC, ns2.node
                            LIMIT 1
                            FOR UPDATE OF ns2 SKIP LOCKED)
                    RETURNING ns.run_id, ns.node
                    """,
                    (self.name, LEASE_SECONDS),
                )
                row = cur.fetchone()
            conn.commit()
            return row
        except Exception as exc:
            conn.rollback()
            self.log.warning("认领任务失败: %s", exc)
            return None
        finally:
            conn.close()

    # ---------- 心跳 ----------
    def heartbeat_loop(self, run_id, node, lost: threading.Event):
        while not lost.wait(HEARTBEAT_SECONDS):
            try:
                conn = db.connect()
                with conn.cursor() as cur:
                    cur.execute(
                        """UPDATE node_states
                              SET lease_expires_at=now()+(%s||' seconds')::interval
                            WHERE run_id=%s AND node=%s AND claimed_by=%s
                              AND status='running'""",
                        (LEASE_SECONDS, run_id, node, self.name),
                    )
                    n = cur.rowcount
                conn.commit()
                conn.close()
                if n == 0:
                    lost.set()  # 运行被替换/任务被回收，停止执行
                    return
            except Exception as exc:
                self.log.warning("心跳失败 run=%s node=%s: %s", run_id, node, exc)
                time.sleep(0.5)

    # ---------- 执行 ----------
    def execute_task(self, run_id, node):
        lost = threading.Event()
        hb = threading.Thread(
            target=self.heartbeat_loop, args=(run_id, node, lost), daemon=True
        )
        hb.start()
        try:
            self._demo_delay(node, lost)
            if lost.is_set():
                self.log.info("run=%s node=%s 租约已失效，放弃执行", run_id, node)
                return
            if node == "publish":
                self._do_publish(run_id)
            else:
                result = self._compute(run_id, node)
                self._commit_success(run_id, node, result)
        except pipeline.NodeError as exc:
            self._commit_failure(run_id, node, str(exc))
        except Exception as exc:
            self.log.exception("run=%s node=%s 内部错误", run_id, node)
            self._commit_failure(run_id, node, f"内部错误: {exc}")
        finally:
            lost.set()

    def _demo_delay(self, node, lost: threading.Event):
        """演示用人为延迟（网页可设置），模拟慢节点。"""
        try:
            conn = db.connect()
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT value FROM demo_settings WHERE key=%s", (f"delay:{node}",)
                )
                row = cur.fetchone()
            conn.close()
            delay = float(row[0]) if row else 0.0
        except Exception:
            delay = 0.0
        if delay <= 0:
            return
        self.log.info("run 节点 %s 人为延迟 %.0f 秒（演示慢节点）", node, delay)
        end = time.monotonic() + delay
        while time.monotonic() < end and not stop_event.is_set() and not lost.is_set():
            time.sleep(min(0.5, end - time.monotonic()))

    def _compute(self, run_id, node):
        conn = db.connect()
        try:
            cur = conn.cursor()
            if node in ("validate_arrivals", "validate_coords"):
                col = (
                    "arrivals_file_id"
                    if node == "validate_arrivals"
                    else "coords_file_id"
                )
                cur.execute(
                    f"SELECT f.content FROM runs r JOIN files f ON f.id=r.{col}"
                    " WHERE r.id=%s",
                    (run_id,),
                )
                content = cur.fetchone()[0]
                parser = (
                    pipeline.parse_arrivals
                    if node == "validate_arrivals"
                    else pipeline.parse_coords
                )
                rows, errors = parser(content)
                if errors:
                    shown = "；".join(errors[:20])
                    if len(errors) > 20:
                        shown += f"；……共 {len(errors)} 处问题"
                    raise pipeline.NodeError(shown)
                return {"rows": rows, "row_count": len(rows)}
            if node == "join":
                a = self._dep_rows(cur, run_id, "validate_arrivals")
                c = self._dep_rows(cur, run_id, "validate_coords")
                merged, missing = pipeline.join_rows(a, c)
                if missing:
                    raise pipeline.NodeError(
                        f"{len(missing)} 个台站缺少坐标: "
                        + ", ".join(missing[:20])
                        + (" …" if len(missing) > 20 else "")
                    )
                return {"rows": merged, "row_count": len(merged)}
            raise pipeline.NodeError(f"未知节点 {node}")
        finally:
            conn.close()

    @staticmethod
    def _dep_rows(cur, run_id, dep):
        cur.execute(
            "SELECT result FROM node_states"
            " WHERE run_id=%s AND node=%s AND status='success'",
            (run_id, dep),
        )
        row = cur.fetchone()
        if not row:
            raise pipeline.NodeError(f"依赖节点 {dep} 结果缺失")
        return row[0]["rows"]

    # ---------- 提交（带迟到保护） ----------
    _GUARD = """
        run_id=%s AND node=%s AND claimed_by=%s AND status='running'
        AND EXISTS (SELECT 1 FROM runs r WHERE r.id=node_states.run_id
                    AND r.status='running')
    """

    def _commit_success(self, run_id, node, result):
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE node_states
                           SET status='success', result=%s, error=NULL,
                               claimed_by=NULL, lease_expires_at=NULL, updated_at=now()
                         WHERE {self._GUARD}""",
                    (Json(result), run_id, node, self.name),
                )
                ok = cur.rowcount == 1
            if ok:
                conn.commit()
                self.log.info("run=%s node=%s 成功", run_id, node)
            else:
                conn.rollback()
                self.log.warning(
                    "run=%s node=%s 结果已丢弃：运行已被替换或租约失效（迟到旧任务）",
                    run_id, node,
                )
        finally:
            conn.close()

    def _commit_failure(self, run_id, node, error):
        conn = db.connect()
        try:
            with conn.cursor() as cur:
                cur.execute(
                    f"""UPDATE node_states
                           SET status='failed', error=%s,
                               claimed_by=NULL, lease_expires_at=NULL, updated_at=now()
                         WHERE {self._GUARD}""",
                    (error, run_id, node, self.name),
                )
                if cur.rowcount == 1:
                    cur.execute(
                        "UPDATE runs SET status='failed'"
                        " WHERE id=%s AND status='running'",
                        (run_id,),
                    )
                    conn.commit()
                    self.log.info("run=%s node=%s 失败: %s", run_id, node, error)
                else:
                    conn.rollback()
                    self.log.warning(
                        "run=%s node=%s 失败结果已丢弃（运行已被替换）", run_id, node
                    )
        finally:
            conn.close()

    def _do_publish(self, run_id):
        """发布：结果写入与状态翻转在同一事务，失败时绝不留半份 CSV。"""
        conn = db.connect()
        try:
            cur = conn.cursor()
            cur.execute(
                "SELECT result FROM node_states"
                " WHERE run_id=%s AND node='join' AND status='success'",
                (run_id,),
            )
            row = cur.fetchone()
            if not row:
                conn.close()
                self._commit_failure(run_id, "publish", "join 结果缺失")
                return
            rows = row[0]["rows"]
            csv_text = pipeline.build_csv(rows)
            cur.execute(
                f"""UPDATE node_states
                       SET status='success', result=%s, error=NULL,
                           claimed_by=NULL, lease_expires_at=NULL, updated_at=now()
                     WHERE {self._GUARD}""",
                (Json({"row_count": len(rows)}), run_id, "publish", self.name),
            )
            if cur.rowcount != 1:
                conn.rollback()
                self.log.warning("run=%s publish 结果已丢弃（运行已被替换）", run_id)
                return
            cur.execute(
                """INSERT INTO results (run_id, csv, row_count)
                   VALUES (%s, %s, %s)
                   ON CONFLICT (run_id) DO UPDATE
                     SET csv=EXCLUDED.csv, row_count=EXCLUDED.row_count""",
                (run_id, csv_text, len(rows)),
            )
            cur.execute(
                "UPDATE runs SET status='published' WHERE id=%s AND status='running'",
                (run_id,),
            )
            if cur.rowcount != 1:
                conn.rollback()
                self.log.warning("run=%s publish 提交被丢弃（运行已被替换）", run_id)
                return
            conn.commit()
            self.log.info("run=%s 已发布完整结果（%d 行）", run_id, len(rows))
        finally:
            conn.close()

    # ---------- 看门线程：心跳上报、租约回收、旧运行清理、退出指令 ----------
    def janitor_loop(self):
        while not stop_event.wait(JANITOR_SECONDS):
            try:
                conn = db.connect()
                cur = conn.cursor()
                cur.execute(
                    """INSERT INTO workers (name, last_seen) VALUES (%s, now())
                       ON CONFLICT (name) DO UPDATE SET last_seen=now()""",
                    (self.name,),
                )
                cur.execute(
                    """UPDATE node_states ns
                          SET status='pending', claimed_by=NULL,
                              lease_expires_at=NULL, updated_at=now()
                         FROM runs r
                        WHERE ns.run_id=r.id AND ns.status='running'
                          AND ns.lease_expires_at < now() AND r.status='running'"""
                )
                if cur.rowcount:
                    self.log.warning("回收过期租约任务 %d 个（原执行进程已退出）", cur.rowcount)
                cur.execute(
                    """UPDATE node_states ns
                          SET status='cancelled', claimed_by=NULL,
                              lease_expires_at=NULL, updated_at=now()
                         FROM runs r
                        WHERE ns.run_id=r.id AND r.status='superseded'
                          AND ns.status IN ('pending','running')"""
                )
                cur.execute(
                    "SELECT value FROM demo_settings WHERE key=%s",
                    (f"exit:{self.name}",),
                )
                row = cur.fetchone()
                if row and row[0] == "1":
                    cur.execute(
                        "DELETE FROM demo_settings WHERE key=%s", (f"exit:{self.name}",)
                    )
                    conn.commit()
                    conn.close()
                    self.log.error("收到退出指令，模拟进程崩溃（租约留待他进程回收）")
                    os._exit(1)  # 不做任何清理，模拟崩溃
                conn.commit()
                conn.close()
            except Exception as exc:
                self.log.warning("看门线程错误: %s", exc)
                time.sleep(1.0)

    def executor_loop(self):
        while not stop_event.is_set():
            task = self.claim_one()
            if not task:
                time.sleep(POLL_SECONDS)
                continue
            run_id, node = task
            self.log.info("认领 run=%s node=%s", run_id, node)
            self.execute_task(run_id, node)

    def run(self):
        db.ensure_schema()
        db.self_stop_watcher()
        self.log.info("执行进程启动，并发线程 %d", EXEC_THREADS)
        threads = [threading.Thread(target=self.janitor_loop, daemon=True)]
        threads += [
            threading.Thread(target=self.executor_loop, daemon=True)
            for _ in range(EXEC_THREADS)
        ]
        for t in threads:
            t.start()
        while not stop_event.is_set():
            time.sleep(0.5)
        self.log.info("收到停止信号，等待在途任务结束……")
        time.sleep(2)


def main():
    name = (
        sys.argv[1]
        if len(sys.argv) > 1
        else os.environ.get("WORKER_NAME", "worker")
    )

    def _sigterm(signum, frame):
        stop_event.set()

    signal.signal(signal.SIGTERM, _sigterm)
    signal.signal(signal.SIGINT, _sigterm)
    Worker(name).run()


if __name__ == "__main__":
    main()
