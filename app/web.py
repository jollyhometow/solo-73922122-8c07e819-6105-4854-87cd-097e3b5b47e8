"""FastAPI 网页：维护一次地震的到时CSV与坐标CSV，查看节点状态、换件、重试失败节点、下载完整结果。

页面只读写 PostgreSQL，不触碰执行进程内部状态，因此慢节点不挡查询与换件。
"""
import html

import psycopg2.extras
from fastapi import FastAPI, File, Form, UploadFile
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse, Response

from . import db, pipeline

app = FastAPI(title="地震定位输入制备")

MAX_UPLOAD_BYTES = 2_000_000


@app.on_event("startup")
def _startup():
    db.ensure_schema()
    db.self_stop_watcher()


# ---------------------------------------------------------------- 状态查询

def build_status() -> dict:
    conn = db.connect()
    try:
        cur = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        files = {}
        for kind in ("arrivals", "coords"):
            cur.execute(
                "SELECT id, version, filename, sha256, uploaded_at"
                " FROM files WHERE kind=%s ORDER BY version DESC LIMIT 1",
                (kind,),
            )
            files[kind] = cur.fetchone()
        cur.execute("SELECT * FROM runs ORDER BY id DESC LIMIT 1")
        run = cur.fetchone()
        nodes = []
        if run:
            cur.execute(
                """SELECT * FROM node_states WHERE run_id=%s
                   ORDER BY CASE node
                     WHEN 'validate_arrivals' THEN 1 WHEN 'validate_coords' THEN 2
                     WHEN 'join' THEN 3 ELSE 4 END""",
                (run["id"],),
            )
            nodes = list(cur.fetchall())
        cur.execute(
            """SELECT name, last_seen,
                      (now() - last_seen) < interval '8 seconds' AS alive
                 FROM workers ORDER BY name"""
        )
        workers = list(cur.fetchall())
        cur.execute("SELECT key, value FROM demo_settings")
        demo = {r["key"]: r["value"] for r in cur.fetchall()}
        cur.execute(
            "SELECT id, status, created_at FROM runs ORDER BY id DESC LIMIT 5"
        )
        recent_runs = list(cur.fetchall())
        result = None
        if run and run["status"] == "published":
            cur.execute(
                "SELECT row_count, created_at FROM results WHERE run_id=%s",
                (run["id"],),
            )
            result = cur.fetchone()
    finally:
        conn.close()

    def norm(v):
        return v.isoformat() if hasattr(v, "isoformat") else v

    def norm_row(r):
        return {k: norm(v) for k, v in r.items()} if r else None

    node_list = []
    for n in nodes:
        d = norm_row(n)
        res = d.pop("result", None) or {}
        d["row_count"] = res.get("row_count")
        d["reused_from_run"] = res.get("reused_from_run")
        d["label"] = pipeline.NODE_LABELS.get(d["node"], d["node"])
        node_list.append(d)
    return {
        "files": {k: norm_row(v) for k, v in files.items()},
        "run": norm_row(run),
        "nodes": node_list,
        "workers": [norm_row(w) for w in workers],
        "demo": demo,
        "recent_runs": [norm_row(r) for r in recent_runs],
        "result": norm_row(result),
        "download_available": bool(run and run["status"] == "published" and result),
    }


@app.get("/api/status")
def api_status():
    return JSONResponse(build_status())


# ---------------------------------------------------------------- 上传（换件）

def create_run(cur, a_id, a_sha, c_id, c_sha) -> int:
    """换件后开新运行：旧运行立即作废（旧结果同时停供）；
    输入指纹相同的成功节点直接复用，只重跑受影响分支及下游。"""
    cur.execute("UPDATE runs SET status='superseded' WHERE status<>'superseded'")
    cur.execute(
        "INSERT INTO runs (arrivals_file_id, coords_file_id) VALUES (%s,%s)"
        " RETURNING id",
        (a_id, c_id),
    )
    run_id = cur.fetchone()[0]
    publish_reused_from = None
    for node in pipeline.NODES:
        ih = pipeline.inputs_hash(node, a_sha, c_sha)
        cur.execute(
            """SELECT run_id, result, attempts FROM node_states
                WHERE node=%s AND inputs_hash=%s AND status='success'
                ORDER BY run_id DESC LIMIT 1""",
            (node, ih),
        )
        row = cur.fetchone()
        if row:
            src_run, result, attempts = row
            result = dict(result or {})
            result["reused_from_run"] = src_run
            cur.execute(
                """INSERT INTO node_states
                       (run_id, node, status, inputs_hash, attempts, result)
                   VALUES (%s,%s,'success',%s,%s,%s)""",
                (run_id, node, ih, attempts, psycopg2.extras.Json(result)),
            )
            if node == "publish":
                publish_reused_from = src_run
        else:
            cur.execute(
                "INSERT INTO node_states (run_id, node, status, inputs_hash)"
                " VALUES (%s,%s,'pending',%s)",
                (run_id, node, ih),
            )
    if publish_reused_from is not None:  # 完全相同的两份文件曾成功发布过
        cur.execute(
            "INSERT INTO results (run_id, csv, row_count)"
            " SELECT %s, csv, row_count FROM results WHERE run_id=%s",
            (run_id, publish_reused_from),
        )
        cur.execute("UPDATE runs SET status='published' WHERE id=%s", (run_id,))
    return run_id


@app.post("/upload/{kind}")
async def upload(kind: str, file: UploadFile = File(...)):
    if kind not in ("arrivals", "coords"):
        return JSONResponse({"error": "kind 应为 arrivals 或 coords"}, 404)
    raw = await file.read()
    if len(raw) > MAX_UPLOAD_BYTES:
        return JSONResponse({"error": "文件过大"}, 413)
    content = raw.decode("utf-8-sig", errors="replace")
    sha = pipeline.sha256_text(content)
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute(
            "SELECT COALESCE(MAX(version),0)+1 FROM files WHERE kind=%s", (kind,)
        )
        version = cur.fetchone()[0]
        cur.execute(
            "INSERT INTO files (kind, version, filename, content, sha256)"
            " VALUES (%s,%s,%s,%s,%s) RETURNING id",
            (kind, version, file.filename or "upload.csv", content, sha),
        )
        fid = cur.fetchone()[0]
        other_kind = "coords" if kind == "arrivals" else "arrivals"
        cur.execute(
            "SELECT id, sha256 FROM files WHERE kind=%s"
            " ORDER BY version DESC LIMIT 1",
            (other_kind,),
        )
        other = cur.fetchone()
        run_id = None
        if other:  # 两份文件齐备才开运行
            if kind == "arrivals":
                run_id = create_run(cur, fid, sha, other[0], other[1])
            else:
                run_id = create_run(cur, other[0], other[1], fid, sha)
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse("/", 303)


# ---------------------------------------------------------------- 重试失败节点

@app.post("/retry/{node}")
def retry(node: str):
    if node not in pipeline.NODES:
        return JSONResponse({"error": "未知节点"}, 404)
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, status FROM runs ORDER BY id DESC LIMIT 1")
        run = cur.fetchone()
        if run and run[1] == "failed":
            run_id = run[0]
            cur.execute(
                "SELECT status FROM node_states WHERE run_id=%s AND node=%s",
                (run_id, node),
            )
            row = cur.fetchone()
            if row and row[0] == "failed":
                cur.execute(
                    "UPDATE runs SET status='running' WHERE id=%s", (run_id,)
                )
                for n in [node] + pipeline.DOWNSTREAM[node]:
                    cur.execute(
                        """UPDATE node_states
                              SET status='pending', error=NULL, result=NULL,
                                  claimed_by=NULL, lease_expires_at=NULL,
                                  updated_at=now()
                            WHERE run_id=%s AND node=%s""",
                        (run_id, n),
                    )
                conn.commit()
    finally:
        conn.close()
    return RedirectResponse("/", 303)


# ---------------------------------------------------------------- 下载

@app.get("/download")
def download():
    """只提供当前运行的完整结果；换件后旧结果立即停供，失败不开放半份 CSV。"""
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute("SELECT id, status FROM runs ORDER BY id DESC LIMIT 1")
        run = cur.fetchone()
        if run and run[1] == "published":
            cur.execute("SELECT csv FROM results WHERE run_id=%s", (run[0],))
            row = cur.fetchone()
            if row:
                return Response(
                    row[0],
                    media_type="text/csv",
                    headers={
                        "Content-Disposition": 'attachment; filename="location_input.csv"'
                    },
                )
        return JSONResponse(
            {
                "error": "当前无可用完整结果（换件重跑中或校验失败）",
                "run_status": run[1] if run else None,
            },
            status_code=409,
        )
    finally:
        conn.close()


# ---------------------------------------------------------------- 演示控制

@app.post("/demo/delay")
def demo_delay(node: str = Form(...), seconds: int = Form(...)):
    if node in pipeline.NODES:
        seconds = max(0, min(300, seconds))
        conn = db.connect()
        try:
            cur = conn.cursor()
            if seconds == 0:
                cur.execute(
                    "DELETE FROM demo_settings WHERE key=%s", (f"delay:{node}",)
                )
            else:
                cur.execute(
                    """INSERT INTO demo_settings (key, value) VALUES (%s,%s)
                       ON CONFLICT (key) DO UPDATE SET value=EXCLUDED.value""",
                    (f"delay:{node}", str(seconds)),
                )
            conn.commit()
        finally:
            conn.close()
    return RedirectResponse("/", 303)


@app.post("/demo/exit")
def demo_exit(worker: str = Form(...)):
    conn = db.connect()
    try:
        cur = conn.cursor()
        cur.execute(
            """INSERT INTO demo_settings (key, value) VALUES (%s,'1')
               ON CONFLICT (key) DO UPDATE SET value='1'""",
            (f"exit:{worker}",),
        )
        conn.commit()
    finally:
        conn.close()
    return RedirectResponse("/", 303)


# ---------------------------------------------------------------- 页面

def _e(s) -> str:
    return html.escape(str(s)) if s is not None else ""


def _badge(status: str) -> str:
    colors = {
        "success": "#1a7f37", "published": "#1a7f37",
        "running": "#0969da", "pending": "#6e7781",
        "failed": "#cf222e", "cancelled": "#6e7781",
        "superseded": "#6e7781",
    }
    c = colors.get(status, "#6e7781")
    return (
        f'<span style="background:{c};color:#fff;padding:2px 8px;'
        f'border-radius:10px;font-size:12px">{_e(status)}</span>'
    )


@app.get("/", response_class=HTMLResponse)
def index():
    st = build_status()
    run = st["run"]
    files = st["files"]
    kind_label = {"arrivals": "到时CSV", "coords": "坐标CSV"}

    file_rows = ""
    for kind in ("arrivals", "coords"):
        f = files[kind]
        if f:
            file_rows += (
                f"<tr><td>{kind_label[kind]}</td><td>v{f['version']}</td>"
                f"<td>{_e(f['filename'])}</td><td><code>{f['sha256'][:12]}</code></td>"
                f"<td>{_e(f['uploaded_at'])[:19]}</td></tr>"
            )
        else:
            file_rows += f"<tr><td>{kind_label[kind]}</td><td colspan=4>未上传</td></tr>"

    node_rows = ""
    for n in st["nodes"]:
        retry_btn = ""
        if run and run["status"] == "failed" and n["status"] == "failed":
            retry_btn = (
                f'<form method="post" action="/retry/{n["node"]}" '
                f'style="display:inline"><button>重试</button></form>'
            )
        reused = (
            f'复用 run#{n["reused_from_run"]}' if n["reused_from_run"] else ""
        )
        node_rows += (
            f"<tr><td>{_e(n['label'])}<br><code>{n['node']}</code></td>"
            f"<td>{_badge(n['status'])}</td>"
            f"<td>{_e(n['claimed_by'] or '')}</td>"
            f"<td>{n['attempts']}</td>"
            f"<td>{n['row_count'] if n['row_count'] is not None else ''}</td>"
            f"<td>{reused}</td>"
            f'<td style="color:#cf222e;max-width:360px;word-break:break-all">'
            f"{_e(n['error'] or '')}</td><td>{retry_btn}</td></tr>"
        )
    if not node_rows:
        node_rows = '<tr><td colspan=8>尚无运行（请先上传两份 CSV）</td></tr>'

    worker_rows = ""
    for w in st["workers"]:
        alive = "🟢 存活" if w["alive"] else "🔴 已退出"
        worker_rows += (
            f"<tr><td>{_e(w['name'])}</td><td>{alive}</td>"
            f"<td>{_e(w['last_seen'])[:19]}</td>"
            f'<td><form method="post" action="/demo/exit" style="display:inline">'
            f'<input type="hidden" name="worker" value="{_e(w["name"])}">'
            f"<button>模拟崩溃</button></form></td></tr>"
        )
    if not worker_rows:
        worker_rows = "<tr><td colspan=4>无执行进程心跳</td></tr>"

    if st["download_available"]:
        dl = (
            f'<a href="/download"><button style="font-size:16px">⬇ 下载完整CSV'
            f"</button></a> 行数 {st['result']['row_count']}，"
            f"发布于 {_e(st['result']['created_at'])[:19]}"
        )
    else:
        dl = "<b>当前无可用完整结果</b>（换件重跑中或校验失败；旧结果已停供）"

    run_info = (
        f"run#{run['id']} {_badge(run['status'])} 创建于 {_e(run['created_at'])[:19]}"
        if run
        else "尚无运行"
    )
    delays = {k[6:]: v for k, v in st["demo"].items() if k.startswith("delay:")}
    delay_txt = (
        "，".join(f"{k} 延迟 {v}s" for k, v in delays.items()) or "无"
    )
    node_options = "".join(
        f'<option value="{n}">{pipeline.NODE_LABELS[n]}</option>'
        for n in pipeline.NODES
    )

    return f"""<!doctype html>
<html lang="zh"><head><meta charset="utf-8">
<meta http-equiv="refresh" content="3">
<title>地震定位输入制备</title>
<style>
 body{{font-family:system-ui,sans-serif;margin:24px;max-width:1100px}}
 table{{border-collapse:collapse;margin:8px 0}}
 td,th{{border:1px solid #d0d7de;padding:4px 10px;font-size:14px}}
 th{{background:#f6f8fa}}
 section{{margin-bottom:28px}}
 button{{cursor:pointer}}
</style></head><body>
<h1>地震定位输入制备</h1>
<section><h2>当前运行</h2><p>{run_info}</p>
<table><tr><th>节点</th><th>状态</th><th>执行者</th><th>尝试</th><th>行数</th>
<th>复用</th><th>错误</th><th>操作</th></tr>{node_rows}</table></section>

<section><h2>完整结果</h2><p>{dl}</p></section>

<section><h2>数据文件（换件）</h2>
<table><tr><th>文件</th><th>版本</th><th>文件名</th><th>SHA256</th><th>上传时间</th></tr>
{file_rows}</table>
<form method="post" action="/upload/arrivals" enctype="multipart/form-data">
 <b>到时CSV</b>（台站号,UTC到时）<input type="file" name="file" required>
 <button>上传并换件</button></form>
<form method="post" action="/upload/coords" enctype="multipart/form-data">
 <b>坐标CSV</b>（台站号,纬度,经度）<input type="file" name="file" required>
 <button>上传并换件</button></form>
<p style="color:#57606a">换件只重跑该分支及下游节点，另一分支的成功校验直接复用；
旧结果立即停供。</p></section>

<section><h2>执行进程</h2>
<table><tr><th>名称</th><th>状态</th><th>最后心跳</th><th>演示</th></tr>
{worker_rows}</table></section>

<section><h2>演示控制</h2>
<p>当前人为延迟：{_e(delay_txt)}</p>
<form method="post" action="/demo/delay">
 节点 <select name="node">{node_options}</select>
 延迟秒数 <input type="number" name="seconds" min="0" max="300" value="20">
 <button>设置（0=清除）</button></form>
<p style="color:#57606a">慢节点不挡查询、换件和另一分支；执行进程崩溃后，
其任务租约到期由另一进程自动接续。页面每 3 秒自动刷新。</p></section>
</body></html>"""
