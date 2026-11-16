"""流水线节点定义与 CSV 校验/关联/发布逻辑（纯函数，便于测试）。

DAG:
    validate_arrivals ─┐
                       ├─> join ─> publish
    validate_coords ───┘
"""
import csv
import hashlib
import io
import re
from datetime import datetime, timezone

MAX_ROWS = 1000
STATION_RE = re.compile(r"^[A-Za-z0-9_.:-]{1,64}$")

NODES = ["validate_arrivals", "validate_coords", "join", "publish"]
NODE_LABELS = {
    "validate_arrivals": "校验到时CSV",
    "validate_coords": "校验坐标CSV",
    "join": "台站关联",
    "publish": "发布完整结果",
}
# 某节点重跑时，其下游节点一并失效重跑
DOWNSTREAM = {
    "validate_arrivals": ["join", "publish"],
    "validate_coords": ["join", "publish"],
    "join": ["publish"],
    "publish": [],
}

# 首行单元格（小写）命中这些词时视为表头跳过
_HEADER_TOKENS = {
    "station", "station_id", "sta", "台站", "台站号",
    "utc_time", "time", "arrival", "arrival_time", "到时", "utc到时",
    "lat", "latitude", "纬度", "lon", "lng", "longitude", "经度",
}


class NodeError(Exception):
    """节点业务失败（校验不通过、缺坐标等），错误信息会展示在网页上。"""


def sha256_text(text: str) -> str:
    return hashlib.sha256(text.encode("utf-8")).hexdigest()


def inputs_hash(node: str, arrivals_sha: str, coords_sha: str) -> str:
    """节点输入指纹：相同指纹的成功结果可直接复用（换件只重跑受影响分支）。"""
    if node == "validate_arrivals":
        return "A:" + arrivals_sha
    if node == "validate_coords":
        return "C:" + coords_sha
    return node[0].upper() + ":" + arrivals_sha + ":" + coords_sha


def _read_rows(content: str):
    reader = csv.reader(io.StringIO(content))
    rows = []
    for raw in reader:
        if not raw or all(not c.strip() for c in raw):
            continue  # 跳过空行
        rows.append([c.strip() for c in raw])
    return rows


def _strip_header(rows):
    if rows and any(cell.lower() in _HEADER_TOKENS for cell in rows[0]):
        return rows[1:]
    return rows


def _parse_time(s: str) -> str:
    """解析 ISO 8601 到时，统一规范化为 UTC 'YYYY-MM-DDTHH:MM:SS[.mmm]Z'。"""
    t = s.strip()
    if t.endswith(("Z", "z")):
        t = t[:-1] + "+00:00"
    dt = datetime.fromisoformat(t)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)  # 列定义为 UTC 到时，无时区按 UTC 处理
    dt = dt.astimezone(timezone.utc)
    out = dt.strftime("%Y-%m-%dT%H:%M:%S")
    if dt.microsecond:
        out += f".{dt.microsecond // 1000:03d}"
    return out + "Z"


def _check_station(st, seen, lineno, errors):
    if not STATION_RE.match(st):
        errors.append(f"第{lineno}行: 台站号非法 {st!r}（允许字母数字_.:-，1-64字符）")
        return False
    if st in seen:
        errors.append(f"第{lineno}行: 台站号重复 {st!r}")
        return False
    seen.add(st)
    return True


def parse_arrivals(content: str):
    """到时CSV：台站号,UTC到时。返回 (rows, errors)，rows=[{station,utc_time}]。"""
    rows = _strip_header(_read_rows(content))
    errors, out, seen = [], [], set()
    if not rows:
        errors.append("CSV 无数据行")
    if len(rows) > MAX_ROWS:
        errors.append(f"数据行数 {len(rows)} 超过上限 {MAX_ROWS}")
    for i, r in enumerate(rows, start=1):
        if len(r) != 2:
            errors.append(f"第{i}行: 应为 2 列（台站号,UTC到时），实得 {len(r)} 列")
            continue
        st, ts = r
        if not _check_station(st, seen, i, errors):
            continue
        try:
            out.append({"station": st, "utc_time": _parse_time(ts)})
        except ValueError:
            errors.append(f"第{i}行: 到时无法解析 {ts!r}（需 ISO 8601，如 2026-11-16T01:02:03Z）")
    return out, errors


def parse_coords(content: str):
    """坐标CSV：台站号,纬度,经度。返回 (rows, errors)，rows=[{station,lat,lon}]。"""
    rows = _strip_header(_read_rows(content))
    errors, out, seen = [], [], set()
    if not rows:
        errors.append("CSV 无数据行")
    if len(rows) > MAX_ROWS:
        errors.append(f"数据行数 {len(rows)} 超过上限 {MAX_ROWS}")
    for i, r in enumerate(rows, start=1):
        if len(r) != 3:
            errors.append(f"第{i}行: 应为 3 列（台站号,纬度,经度），实得 {len(r)} 列")
            continue
        st, la, lo = r
        if not _check_station(st, seen, i, errors):
            continue
        try:
            lat = float(la)
            if not -90.0 <= lat <= 90.0:
                raise ValueError
        except ValueError:
            errors.append(f"第{i}行: 纬度非法 {la!r}（-90~90）")
            continue
        try:
            lon = float(lo)
            if not -180.0 <= lon <= 180.0:
                raise ValueError
        except ValueError:
            errors.append(f"第{i}行: 经度非法 {lo!r}（-180~180）")
            continue
        out.append({"station": st, "lat": lat, "lon": lon})
    return out, errors


def join_rows(arrivals, coords):
    """按台站号关联。返回 (merged, missing)，missing 为缺坐标的台站号列表。"""
    cmap = {r["station"]: r for r in coords}
    merged, missing = [], []
    for r in arrivals:
        c = cmap.get(r["station"])
        if c is None:
            missing.append(r["station"])
        else:
            merged.append(
                {
                    "station": r["station"],
                    "utc_time": r["utc_time"],
                    "lat": c["lat"],
                    "lon": c["lon"],
                }
            )
    return merged, missing


def build_csv(merged) -> str:
    buf = io.StringIO()
    w = csv.writer(buf, lineterminator="\n")
    w.writerow(["station", "utc_time", "lat", "lon"])
    for r in merged:
        w.writerow([r["station"], r["utc_time"], f'{r["lat"]:g}', f'{r["lon"]:g}'])
    return buf.getvalue()
