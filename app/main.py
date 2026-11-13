"""
遥测恢复演练台（FastAPI）

核心流程（真实物理恢复，非 SQL 重建）：
  1. 复制 pg_basebackup 物理基线备份到独立运行目录
  2. 写入恢复配置：restore_command 指向 WAL 归档、recovery_target_name 精确停在
     所选命名恢复点、recovery_target_action=promote
  3. 以独立 postgres 实例（容器内 127.0.0.1:5433）重放 WAL
  4. 三重证据确认“准确到达目标”后才开放结果：
       - 日志出现 recovery stopping at restore point "<所选恢复点>"
       - 日志出现 archive recovery complete
       - SQL 查询 pg_is_in_recovery() = false（已 promote）
     仅凭数据库可连接一律不算成功
  5. 归档缺段 / 未到达目标 -> FAILED：立即停止并回收恢复进程，残库不开放查询，
     必须显式“回收残库”后才允许重试
  6. 全站同时只允许一份恢复（状态机 + 锁）
"""
import json
import os
import re
import shutil
import subprocess
import threading
import time
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

import psycopg
from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import FileResponse
from psycopg.rows import dict_row
from pydantic import BaseModel

PG_BIN = os.environ.get("PG_BIN", "/usr/lib/postgresql/16/bin")
PRIMARY_DSN = os.environ.get(
    "PRIMARY_DSN",
    "host=primary port=5432 dbname=telemetry user=postgres password=postgres",
)
BASEBACKUP_DIR = Path(os.environ.get("BASEBACKUP_DIR", "/basebackup/base"))
MANIFEST_PATH = Path(os.environ.get("MANIFEST_PATH", "/basebackup/manifest.json"))
RUNS_DIR = Path(os.environ.get("RUNS_DIR", "/recovery"))
RESTORE_SCRIPT = os.environ.get("RESTORE_SCRIPT", "/opt/app/restore_command.sh")
RECOVERY_PORT = int(os.environ.get("RECOVERY_PORT", "5433"))
RECOVERY_DB = os.environ.get("RECOVERY_DB", "telemetry")
RECOVERY_USER = os.environ.get("RECOVERY_USER", "postgres")
RECOVERY_PASSWORD = os.environ.get("RECOVERY_PASSWORD", "postgres")
RECOVERY_TIMEOUT = int(os.environ.get("RECOVERY_TIMEOUT", "300"))
STATIC_DIR = Path(__file__).resolve().parent / "static"

IDLE, PREPARING, RECOVERING, VERIFYING, READY, FAILED = (
    "IDLE", "PREPARING", "RECOVERING", "VERIFYING", "READY", "FAILED",
)


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class Manager:
    """全站单恢复状态机。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.state = IDLE
        self.run = None

    def event(self, msg: str):
        print(f"[recovery] {msg}", flush=True)
        if self.run is not None:
            self.run["events"].append({"ts": now_iso(), "msg": msg})


mgr = Manager()


def load_manifest() -> dict:
    if not MANIFEST_PATH.exists():
        raise HTTPException(503, "setup 尚未完成：未找到 /basebackup/manifest.json")
    try:
        return json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    except Exception as e:
        raise HTTPException(500, f"清单解析失败: {e}")


def recovered_dsn(extra: str = "") -> str:
    return (
        f"host=127.0.0.1 port={RECOVERY_PORT} dbname={RECOVERY_DB} "
        f"user={RECOVERY_USER} password={RECOVERY_PASSWORD} connect_timeout=2 {extra}"
    )


def run_cmd(args, timeout=120) -> subprocess.CompletedProcess:
    return subprocess.run(args, capture_output=True, text=True, timeout=timeout)


def stop_postgres(pgdata: Path):
    """停止恢复实例（回收恢复进程）。失败残库必须不可再被查询。"""
    if not pgdata or not pgdata.exists():
        return
    try:
        run_cmd(
            ["gosu", "postgres", f"{PG_BIN}/pg_ctl", "-D", str(pgdata),
             "stop", "-m", "immediate", "-w", "-t", "30"],
            timeout=60,
        )
    except Exception as e:
        print(f"[recovery] stop_postgres error: {e}", flush=True)


def tail_file(path: Path, n=80, max_bytes=65536) -> str:
    try:
        with open(path, "rb") as f:
            f.seek(0, 2)
            size = f.tell()
            f.seek(max(0, size - max_bytes))
            data = f.read().decode("utf-8", "replace")
        return "\n".join(data.splitlines()[-n:])
    except Exception:
        return ""


def fail(run: dict, msg: str, pgdata: Path | None = None):
    """进入 FAILED：先停止并回收恢复进程，残库不开放查询；重试需先回收残库。"""
    mgr.event("失败：" + msg)
    if pgdata is not None:
        stop_postgres(pgdata)
    with mgr.lock:
        run["error"] = msg
        run["finished_at"] = now_iso()
        mgr.state = FAILED


def do_recovery(run: dict):
    rundir = Path(run["rundir"])
    pgdata = rundir / "pgdata"
    logfile = rundir / "postgres.log"
    rp = run["restore_point"]
    try:
        # ---- 1. 复制物理基线备份 ----
        mgr.event(f"复制物理基线备份 {BASEBACKUP_DIR} -> {pgdata}")
        shutil.copytree(BASEBACKUP_DIR, pgdata, symlinks=True)

        # ---- 2. 写入恢复配置（WAL 归档重放 + 命名恢复点精确停止） ----
        missing = run.get("missing_segment") or ""
        restore_cmd = f"{RESTORE_SCRIPT} %f %p" + (f" {missing}" if missing else "")
        if missing:
            mgr.event(f"注入归档缺段故障：段 {missing} 将在 restore_command 中缺失")
        with open(pgdata / "postgresql.auto.conf", "a", encoding="utf-8") as f:
            f.write("\n# ---- recovery drill ----\n")
            f.write(f"port = {RECOVERY_PORT}\n")
            f.write("listen_addresses = '127.0.0.1'\n")
            f.write("unix_socket_directories = '/tmp'\n")
            f.write("archive_mode = off\n")
            f.write(f"restore_command = '{restore_cmd}'\n")
            f.write(f"recovery_target_name = '{rp}'\n")
            f.write("recovery_target_action = 'promote'\n")
            f.write("hot_standby = on\n")
            f.write("default_transaction_read_only = on\n")
        (pgdata / "recovery.signal").touch()
        mgr.event(f"恢复配置就绪：recovery_target_name='{rp}'，restore_command 指向 WAL 归档")

        run_cmd(["chown", "-R", "postgres:postgres", str(pgdata)])
        run_cmd(["chown", "postgres:postgres", str(rundir)])
        run_cmd(["chmod", "700", str(pgdata)])

        # ---- 3. 启动恢复实例，开始重放 WAL ----
        mgr.event("启动恢复实例，开始从 WAL 归档重放")
        with mgr.lock:
            mgr.state = RECOVERING
        r = run_cmd(
            ["gosu", "postgres", f"{PG_BIN}/pg_ctl", "-D", str(pgdata),
             "-l", str(logfile), "-w", "-t", "120", "start"],
            timeout=150,
        )
        if r.returncode != 0:
            fail(run,
                 f"恢复实例未能进入服务状态（pg_ctl 退出码 {r.returncode}）。"
                 "通常为归档缺段或未到达目标恢复点，详见日志。",
                 pgdata)
            return
        poll_until_ready(run, pgdata, logfile, rp)
    except Exception as e:
        fail(run, f"恢复过程异常：{e}", pgdata)


# 轮询连接在恢复早期会被拒，这类连接级 FATAL 是良性的，不能据此判失败
BENIGN_FATALS = (
    "the database system is not yet accepting connections",
    "the database system is starting up",
    "the database system is in recovery mode",
)


def poll_until_ready(run: dict, pgdata: Path, logfile: Path, rp: str):
    deadline = time.time() + RECOVERY_TIMEOUT
    target_re = re.compile(r'recovery stopping at restore point "%s"' % re.escape(rp))
    while time.time() < deadline:
        log = tail_file(logfile, n=400, max_bytes=524288)
        if "recovery ended before configured recovery target was reached" in log:
            fail(run, "未到达目标恢复点：WAL 重放提前结束（归档缺段或目标不在归档中）。", pgdata)
            return
        # 非良性 FATAL 需进程确认退出后才判失败（避免连接期误报）
        fatals = [f for f in re.findall(r"FATAL:\s+(.+)", log)
                  if not any(b in f for b in BENIGN_FATALS)]
        if fatals:
            time.sleep(2)
            st = run_cmd([f"{PG_BIN}/pg_ctl", "-D", str(pgdata), "status"])
            if st.returncode != 0:
                fail(run, f"恢复实例报错：{fatals[-1].strip()}", pgdata)
                return
        # 热备模式下可在重放期间只读连接，用于展示进度；可连接绝不等于成功
        try:
            with psycopg.connect(recovered_dsn(), autocommit=True) as conn:
                in_rec, lsn, ts = conn.execute(
                    "SELECT pg_is_in_recovery(), pg_last_wal_replay_lsn(), "
                    "pg_last_xact_replay_timestamp()"
                ).fetchone()
            run["replay_lsn"] = str(lsn) if lsn is not None else None
            run["replay_ts"] = str(ts) if ts is not None else None
            if not in_rec:
                # 已 promote —— 必须再核对日志中的目标落点证据
                with mgr.lock:
                    mgr.state = VERIFYING
                mgr.event("实例已提升（promote），核对目标恢复点落点证据…")
                log = tail_file(logfile, n=400, max_bytes=524288)
                if target_re.search(log) and "archive recovery complete" in log:
                    gather_results(run)
                    with mgr.lock:
                        mgr.state = READY
                        run["finished_at"] = now_iso()
                    mgr.event(f"已确认到达目标恢复点 {rp}，结果开放查询")
                else:
                    fail(run, "实例已提升但日志缺少目标恢复点落点记录，拒绝开放结果。", pgdata)
                return
        except psycopg.OperationalError:
            pass
        except Exception as e:
            print(f"[recovery] poll error: {e}", flush=True)
        # 进程异常退出且未就绪 -> 失败
        st = run_cmd([f"{PG_BIN}/pg_ctl", "-D", str(pgdata), "status"])
        if st.returncode != 0:
            log = tail_file(logfile, n=400, max_bytes=524288)
            if "database system is ready to accept connections" not in log:
                fail(run, "恢复实例进程退出且未报告就绪。", pgdata)
                return
        time.sleep(1.5)
    fail(run, f"超过 {RECOVERY_TIMEOUT}s 仍未到达目标恢复点。", pgdata)


def gather_results(run: dict):
    manifest = load_manifest()
    rpinfo = next(r for r in manifest["restore_points"] if r["name"] == run["restore_point"])
    ds = manifest["deleted_set"]
    with psycopg.connect(recovered_dsn(), autocommit=True) as conn:
        total = conn.execute("SELECT count(*) FROM telemetry_events").fetchone()[0]
        ds_rows = conn.execute(
            "SELECT count(*) FROM telemetry_events WHERE device_id=%s AND metric=%s",
            (ds["device_id"], ds["metric"]),
        ).fetchone()[0]
        mn, mx = conn.execute(
            "SELECT min(recorded_at), max(recorded_at) FROM telemetry_events"
        ).fetchone()
        # 与清单预期值在数据库内做精确比较，避免时区字符串格式差异造成误判
        mx_match = conn.execute(
            "SELECT max(recorded_at) IS NOT DISTINCT FROM %s::timestamptz "
            "FROM telemetry_events",
            (rpinfo["max_recorded_at"],),
        ).fetchone()[0]
        matrix = conn.execute(
            "SELECT device_id, metric, count(*) FROM telemetry_events "
            "GROUP BY 1,2 ORDER BY 1,2"
        ).fetchall()
    checks = [
        {"label": "遥测总行数", "actual": total,
         "expected": rpinfo["expected_total"], "ok": total == rpinfo["expected_total"]},
        {"label": f"误删集行数（{ds['device_id']} / {ds['metric']}）", "actual": ds_rows,
         "expected": rpinfo["deleted_set_rows"], "ok": ds_rows == rpinfo["deleted_set_rows"]},
        {"label": "最新 recorded_at", "actual": str(mx),
         "expected": rpinfo["max_recorded_at"], "ok": bool(mx_match)},
    ]
    run["result"] = {
        "restore_point": rpinfo["name"],
        "total": total,
        "deleted_set_rows": ds_rows,
        "min_recorded_at": str(mn),
        "max_recorded_at": str(mx),
        "matrix": [{"device_id": d, "metric": m, "count": c} for d, m, c in matrix],
        "expected": rpinfo,
        "checks": checks,
    }


# ---------------------------------------------------------------------------
# 应用生命周期：启动时清理上一轮遗留的恢复实例，保证“全站同时只恢复一份”
# ---------------------------------------------------------------------------
@asynccontextmanager
async def lifespan(app: FastAPI):
    RUNS_DIR.mkdir(parents=True, exist_ok=True)
    for d in sorted(RUNS_DIR.glob("run_*")):
        pgdata = d / "pgdata"
        if (pgdata / "postmaster.pid").exists():
            stop_postgres(pgdata)
        shutil.rmtree(d, ignore_errors=True)
    yield


app = FastAPI(title="遥测恢复演练台", lifespan=lifespan,
              docs_url=None, redoc_url=None, openapi_url=None)


class RecoverRequest(BaseModel):
    restore_point: str
    inject_fault: bool = False


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def api_health():
    return {"ok": True}


@app.get("/api/manifest")
def api_manifest():
    return load_manifest()


@app.get("/api/primary/stats")
def api_primary_stats():
    """原库实时状态：证明演练期间原库持续入仓、不受影响。"""
    try:
        with psycopg.connect(PRIMARY_DSN, autocommit=True) as conn:
            total, latest = conn.execute(
                "SELECT count(*), max(recorded_at) FROM telemetry_events"
            ).fetchone()
            live = conn.execute(
                "SELECT count(*) FROM telemetry_events WHERE device_id='WX-LIVE'"
            ).fetchone()[0]
            arch = conn.execute(
                "SELECT last_archived_wal, last_archived_time, archived_count, failed_count "
                "FROM pg_stat_archiver"
            ).fetchone()
        return {
            "reachable": True,
            "total": total,
            "live_rows": live,
            "latest_recorded_at": str(latest),
            "archiver": {
                "last_archived_wal": arch[0],
                "last_archived_time": str(arch[1]),
                "archived_count": arch[2],
                "failed_count": arch[3],
            },
        }
    except Exception as e:
        return {"reachable": False, "error": str(e)}


@app.post("/api/recover")
def api_recover(req: RecoverRequest):
    manifest = load_manifest()
    rp = next((r for r in manifest["restore_points"] if r["name"] == req.restore_point), None)
    if rp is None:
        raise HTTPException(400, f"未知恢复点: {req.restore_point}")
    if not BASEBACKUP_DIR.exists():
        raise HTTPException(500, "物理基线备份不存在，无法恢复")
    with mgr.lock:
        if mgr.state != IDLE:
            raise HTTPException(
                409,
                f"当前状态为 {mgr.state}：全站同时只允许一份恢复。"
                + ("请先回收失败残库。" if mgr.state == FAILED else "请先回收当前恢复库。"),
            )
        run_id = time.strftime("run_%Y%m%d_%H%M%S_") + req.restore_point
        rundir = RUNS_DIR / run_id
        rundir.mkdir(parents=True, exist_ok=False)
        mgr.run = {
            "id": run_id,
            "restore_point": req.restore_point,
            "inject_fault": req.inject_fault,
            "missing_segment": manifest.get("first_wal_after_backup") if req.inject_fault else None,
            "started_at": now_iso(),
            "finished_at": None,
            "rundir": str(rundir),
            "events": [],
            "replay_lsn": None,
            "replay_ts": None,
            "error": None,
            "result": None,
        }
        mgr.state = PREPARING
        mgr.event(
            f"创建恢复任务 {run_id}，目标恢复点 {req.restore_point}"
            + ("（注入归档缺段故障）" if req.inject_fault else "")
        )
    threading.Thread(target=do_recovery, args=(mgr.run,), daemon=True).start()
    return {"ok": True, "run_id": run_id}


@app.get("/api/status")
def api_status():
    with mgr.lock:
        state = mgr.state
        run = dict(mgr.run) if mgr.run else None
    log_tail = ""
    if run:
        log_tail = tail_file(Path(run["rundir"]) / "postgres.log", 80)
    return {"state": state, "run": run, "log_tail": log_tail}


@app.post("/api/reclaim")
def api_reclaim():
    """回收：停止恢复实例并删除残库。READY/FAILED 状态下调用后回到 IDLE 才允许再次恢复。"""
    with mgr.lock:
        if mgr.state not in (READY, FAILED):
            raise HTTPException(409, f"当前状态 {mgr.state} 无需回收")
        run = mgr.run
    stop_postgres(Path(run["rundir"]) / "pgdata")
    shutil.rmtree(run["rundir"], ignore_errors=True)
    with mgr.lock:
        mgr.state = IDLE
        mgr.run = None
    return {"ok": True}


def require_ready() -> dict:
    """结果开放门禁：仅在已确认到达目标恢复点（READY）时放行；失败残库一律拒绝。"""
    with mgr.lock:
        if mgr.state != READY or mgr.run is None or mgr.run.get("result") is None:
            raise HTTPException(409, "恢复库未确认到达目标恢复点，结果不开放查询")
        return mgr.run


@app.get("/api/recovered/summary")
def api_recovered_summary():
    run = require_ready()
    return run["result"]


@app.get("/api/recovered/rows")
def api_recovered_rows(
    device_id: str | None = None,
    metric: str | None = None,
    limit: int = Query(50, ge=1, le=500),
):
    require_ready()
    for v in (device_id, metric):
        if v is not None and not re.fullmatch(r"[A-Za-z0-9_\-]{1,40}", v):
            raise HTTPException(400, "非法过滤参数")
    sql = ("SELECT id, device_id, site, metric, value, recorded_at, ingested_at "
           "FROM telemetry_events")
    conds, params = [], []
    if device_id:
        conds.append("device_id = %s")
        params.append(device_id)
    if metric:
        conds.append("metric = %s")
        params.append(metric)
    if conds:
        sql += " WHERE " + " AND ".join(conds)
    sql += " ORDER BY recorded_at DESC, id DESC LIMIT %s"
    params.append(limit)
    try:
        with psycopg.connect(
            recovered_dsn("options='-c default_transaction_read_only=on'"),
            autocommit=True,
            row_factory=dict_row,
        ) as conn:
            rows = conn.execute(sql, params).fetchall()
    except psycopg.OperationalError as e:
        raise HTTPException(503, f"恢复库暂不可达: {e}")
    for r in rows:
        r["recorded_at"] = str(r["recorded_at"])
        r["ingested_at"] = str(r["ingested_at"])
    return {"rows": rows}
