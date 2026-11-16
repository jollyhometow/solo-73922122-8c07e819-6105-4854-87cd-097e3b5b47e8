"""数据库连接与建表。所有状态保存在 PostgreSQL 中，网页与两个执行进程均为无状态。"""
import os
import threading
import time

import psycopg2
import psycopg2.extras

SCHEMA_SQL = """
CREATE TABLE IF NOT EXISTS files (
  id          SERIAL PRIMARY KEY,
  kind        TEXT NOT NULL CHECK (kind IN ('arrivals','coords')),
  version     INT  NOT NULL,
  filename    TEXT NOT NULL,
  content     TEXT NOT NULL,
  sha256      TEXT NOT NULL,
  uploaded_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE(kind, version)
);

CREATE TABLE IF NOT EXISTS runs (
  id               SERIAL PRIMARY KEY,
  arrivals_file_id INT NOT NULL REFERENCES files(id),
  coords_file_id   INT NOT NULL REFERENCES files(id),
  status           TEXT NOT NULL DEFAULT 'running'
                   CHECK (status IN ('running','published','failed','superseded')),
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS node_states (
  run_id           INT  NOT NULL REFERENCES runs(id) ON DELETE CASCADE,
  node             TEXT NOT NULL,
  status           TEXT NOT NULL DEFAULT 'pending'
                   CHECK (status IN ('pending','running','success','failed','cancelled')),
  inputs_hash      TEXT NOT NULL,
  claimed_by       TEXT,
  lease_expires_at TIMESTAMPTZ,
  attempts         INT  NOT NULL DEFAULT 0,
  result           JSONB,
  error            TEXT,
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (run_id, node)
);
CREATE INDEX IF NOT EXISTS idx_node_states_claim ON node_states(status, run_id);
CREATE INDEX IF NOT EXISTS idx_node_states_reuse ON node_states(node, inputs_hash, status);

-- 节点依赖（静态 DAG）：join 依赖两份校验，publish 依赖 join
CREATE TABLE IF NOT EXISTS node_deps (
  node       TEXT NOT NULL,
  depends_on TEXT NOT NULL,
  PRIMARY KEY (node, depends_on)
);
INSERT INTO node_deps(node, depends_on) VALUES
  ('join','validate_arrivals'),
  ('join','validate_coords'),
  ('publish','join')
ON CONFLICT DO NOTHING;

-- 只有整次成功后才写入完整结果，失败时此表无记录，下载即不可用（不出半份 CSV）
CREATE TABLE IF NOT EXISTS results (
  run_id     INT PRIMARY KEY REFERENCES runs(id) ON DELETE CASCADE,
  csv        TEXT NOT NULL,
  row_count  INT  NOT NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS workers (
  name      TEXT PRIMARY KEY,
  last_seen TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- 演示控制：delay:<node> 人为延迟秒数；exit:<worker> 让指定执行进程崩溃退出
CREATE TABLE IF NOT EXISTS demo_settings (
  key   TEXT PRIMARY KEY,
  value TEXT NOT NULL
);
"""

ADVISORY_LOCK_ID = 727272


def dsn() -> str:
    return os.environ.get(
        "DATABASE_URL", "postgresql://postgres:postgres@localhost:5432/seismic"
    )


def connect():
    conn = psycopg2.connect(dsn())
    conn.autocommit = False
    return conn


def ensure_schema(retries: int = 30, wait: float = 2.0) -> None:
    """建表（幂等）。服务启动时数据库可能尚未就绪，做有限次重试。"""
    last = None
    for _ in range(retries):
        try:
            conn = connect()
            try:
                with conn.cursor() as cur:
                    cur.execute("SELECT pg_advisory_lock(%s)", (ADVISORY_LOCK_ID,))
                    cur.execute(SCHEMA_SQL)
                    cur.execute("SELECT pg_advisory_unlock(%s)", (ADVISORY_LOCK_ID,))
                conn.commit()
            finally:
                conn.close()
            return
        except Exception as exc:  # 数据库未就绪
            last = exc
            time.sleep(wait)
    raise RuntimeError(f"数据库不可用: {last}")


def self_stop_watcher() -> None:
    """开发/演示辅助：SELF_STOP_FILE 指向的文件出现时进程自行退出。

    某些环境（如无 Docker 的沙箱）无法向进程发送信号，用此文件模拟
    “服务停止/重启”。生产部署不设置该环境变量即可。
    """
    path = os.environ.get("SELF_STOP_FILE")
    if not path:
        return

    def _watch():
        while True:
            if os.path.exists(path):
                os._exit(0)
            time.sleep(1)

    threading.Thread(target=_watch, daemon=True).start()
