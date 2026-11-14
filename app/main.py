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
  7. 正常关闭收尾：开始关闭后不再受理恢复，准备中任务不得继续启动数据库；
     与实例启动交错时迟到创建的实例也须停止；恢复线程与本次启动命令全部结束、
     所属恢复数据库确认停止后才删除副本；迟到完成不开放结果、不写回 READY
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
# 可控关闭演示：DEMO_CLOSE_POINT ∈ {copy, handoff, replay}，在该阶段停住
# DEMO_CLOSE_DELAY 秒，供外部触发关闭；设为空则完全不影响正常演练。
DEMO_CLOSE_POINT = os.environ.get("DEMO_CLOSE_POINT", "").strip().lower()
try:
    DEMO_CLOSE_DELAY = float(os.environ.get("DEMO_CLOSE_DELAY", "0") or 0)
except ValueError:
    DEMO_CLOSE_DELAY = 0.0
# “重放中”演示：每段 WAL 重放延迟，拉长重放窗口（毫秒）
try:
    REPLAY_DEMO_DELAY_MS = int(os.environ.get("REPLAY_DEMO_DELAY_MS", "800") or 0)
except ValueError:
    REPLAY_DEMO_DELAY_MS = 0
STATIC_DIR = Path(__file__).resolve().parent / "static"

IDLE, PREPARING, RECOVERING, VERIFYING, READY, FAILED = (
    "IDLE", "PREPARING", "RECOVERING", "VERIFYING", "READY", "FAILED",
)

# 关闭收尾阶段（不对外展示为可操作状态）：
#   SHUTTING_DOWN —— 已开始关闭：不再受理恢复，在途任务不得启动数据库
#   STOPPING      —— 已通知在途任务停止：等待线程、pg_ctl、postmaster 全部结束
SHUTTING_DOWN, STOPPING = "SHUTTING_DOWN", "STOPPING"


def now_iso() -> str:
    return datetime.now().astimezone().isoformat(timespec="seconds")


class Manager:
    """全站单恢复状态机（含关闭收尾协调）。"""

    def __init__(self):
        self.lock = threading.Lock()
        self.state = IDLE
        self.run = None
        self.closing = False          # True 后不再受理任何恢复请求
        self.threads = {}             # run_id -> 恢复线程
        self.child_procs = {}         # 命令键 -> 正在执行的启动/停止命令
        self.active_pgdata = {}       # run_id -> 恢复实例数据目录（含迟到创建）
        self.cancel = threading.Event()
        # 可控演示停点：运行时可武装，默认取环境变量（正常关闭为 None）
        self.demo_point = DEMO_CLOSE_POINT or None
        self.demo_delay = DEMO_CLOSE_DELAY

    def event(self, msg: str):
        print(f"[recovery] {msg}", flush=True)
        run = self.run
        if run is not None:
            run["events"].append({"ts": now_iso(), "msg": msg})


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


def start_postgres(run: dict, pgdata: Path, logfile: Path):
    """启动恢复实例。

    进程创建与“关闭开始”在同一把锁下判定：要么实例在关闭开始前真正被创建
    （关闭流程会等这条命令结束并停止该实例），要么看到 closing 直接放弃启动。
    因此不存在“关闭已开始后又冒出一次启动”的窗口。
    """
    args = ["gosu", "postgres", f"{PG_BIN}/pg_ctl", "-D", str(pgdata),
            "-l", str(logfile), "-w", "-t", "120", "start"]
    with mgr.lock:
        if mgr.closing:
            return None
        try:
            proc = subprocess.Popen(
                args, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
                text=True, start_new_session=False,
            )
        except OSError as e:
            return subprocess.CompletedProcess(args, 127, "", str(e))
        mgr.child_procs[run["id"]] = proc
    try:
        out, _ = proc.communicate(timeout=150)
    except subprocess.TimeoutExpired:
        proc.kill()
        out, _ = proc.communicate()
        return subprocess.CompletedProcess(args, 124, out or "", "")
    finally:
        with mgr.lock:
            mgr.child_procs.pop(run["id"], None)
    return subprocess.CompletedProcess(args, proc.returncode, out or "", "")


# 关闭收尾等待上限：超过则记录异常，绝不提前放行 READY / 绝不提前删除副本
# 两者之和须小于容器 stop_grace_period，避免被 Docker 强杀掩盖真实收尾结果
SHUTDOWN_THREAD_GRACE = int(os.environ.get("SHUTDOWN_THREAD_GRACE", "60"))


def stop_postgres(pgdata: Path, run_id: str | None = None):
    """停止恢复实例（回收恢复进程）。失败残库必须不可再被查询。

    返回 True 表示确认实例已停止（或本就未运行），False 表示停止失败。
    """
    if not pgdata or not pgdata.exists():
        return True
    args = ["gosu", "postgres", f"{PG_BIN}/pg_ctl", "-D", str(pgdata),
            "stop", "-m", "immediate", "-w", "-t", "30"]
    key = f"{run_id}:stop" if run_id else None
    try:
        if key is not None:
            # 停止命令单独登记，不覆盖同一任务的启动命令登记
            with mgr.lock:
                mgr.child_procs[key] = args
        r = run_cmd(args, timeout=60)
    except Exception as e:
        print(f"[recovery] stop_postgres error: {e}", flush=True)
        return False
    finally:
        if key is not None:
            with mgr.lock:
                mgr.child_procs.pop(key, None)
    return r.returncode == 0


def postmaster_alive(pgdata: Path) -> bool:
    """判断该恢复实例是否仍在运行（不依赖 gosu）。

    以 postmaster.pid + kill(0) 判定，并交叉核对 /proc 命令行，避免 PID 被复用
    时把无关进程误认为恢复实例；僵尸进程（已退出待回收）不算存活。
    """
    try:
        first = (pgdata / "postmaster.pid").read_text(encoding="utf-8").splitlines()[0]
        pid = int(first.strip())
        os.kill(pid, 0)
    except (OSError, ValueError, IndexError, FileNotFoundError):
        return False
    try:
        stat = Path(f"/proc/{pid}/stat").read_text()
        state = stat.rsplit(")", 1)[1].split()[0]
        if state == "Z":
            return False
        cmdline = Path(f"/proc/{pid}/cmdline").read_bytes()
        cmdline = cmdline.replace(b"\0", b" ").decode("utf-8", "replace")
        # postmaster 的命令行包含其数据目录；不含则多半是 PID 复用
        if str(pgdata) not in cmdline:
            return False
    except (OSError, IndexError):
        pass
    return True


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


def cancel_requested() -> bool:
    return mgr.cancel.is_set()


def fail(run: dict, msg: str, pgdata: Path | None = None):
    """进入 FAILED：先停止并回收恢复进程，残库不开放查询；重试需先回收残库。"""
    mgr.event("失败：" + msg)
    if pgdata is not None:
        stop_postgres(pgdata, run_id=run["id"])
    with mgr.lock:
        # 关闭开始后只做收尾，不得把状态写回 FAILED/READY 等对外结果
        if mgr.closing:
            return
        run["error"] = msg
        run["finished_at"] = now_iso()
        mgr.state = FAILED


def claim_pgdata(run: dict, pgdata: Path) -> bool:
    """登记/复核恢复实例目录。关闭开始后拒绝登记，要求收回已创建的目录。"""
    with mgr.lock:
        if mgr.closing:
            return False
        mgr.active_pgdata[run["id"]] = pgdata
        return True


def copy_basebackup(run: dict, src: Path, dst: Path) -> bool:
    """分块复制物理基线；关闭开始后立即中止，不留下半成品被启动。"""
    files: list[Path] = []
    total = 0
    for root, dirs, names in os.walk(src):
        # 先建目录（含空目录，如 pg_wal/archive_status），否则恢复实例无法启动
        for d in dirs:
            rel = Path(root, d).relative_to(src)
            (dst / rel).mkdir(parents=True, exist_ok=True)
        for n in names:
            p = Path(root) / n
            files.append(p)
            try:
                total += p.lstat().st_size
            except OSError:
                pass
    dst.mkdir(parents=True, exist_ok=True)
    done = 0
    once = run.setdefault("_demo_once", {})
    for p in files:
        if cancel_requested():
            mgr.event("关闭已开始，中止基线复制（不会启动数据库）")
            return False
        rel = p.relative_to(src)
        out = dst / rel
        out.parent.mkdir(parents=True, exist_ok=True)
        if p.is_symlink():
            os.symlink(os.readlink(p), out)
        else:
            shutil.copy2(p, out)
            try:
                done += p.stat().st_size
            except OSError:
                pass
        # 复制进行到约 1/5 时进入可控停点（仅演示模式生效）
        if total and done >= total * 0.2:
            demo_hold("copy", once)
        if cancel_requested():
            mgr.event("关闭已开始，中止基线复制（不会启动数据库）")
            return False
    return True


def do_recovery(run: dict):
    try:
        _do_recovery(run)
    finally:
        with mgr.lock:
            th = mgr.threads.get(run["id"])
            if th is not None and th is threading.current_thread():
                mgr.threads.pop(run["id"], None)


def _do_recovery(run: dict):
    rundir = Path(run["rundir"])
    pgdata = rundir / "pgdata"
    logfile = rundir / "postgres.log"
    rp = run["restore_point"]
    try:
        # ---- 0. 关闭协调：开始关闭后不再受理/继续恢复 ----
        if cancel_requested():
            mgr.event("已在关闭收尾中，任务不再受理，直接收尾")
            return
        if not claim_pgdata(run, pgdata):
            mgr.event("关闭已开始，任务被拒绝，不创建副本")
            return

        # ---- 1. 复制物理基线备份 ----
        mgr.event(f"复制物理基线备份 {BASEBACKUP_DIR} -> {pgdata}")
        if not copy_basebackup(run, BASEBACKUP_DIR, pgdata):
            return

        # ---- 2. 写入恢复配置（WAL 归档重放 + 命名恢复点精确停止） ----
        # 关闭可能在复制期间发生：每条可写操作前后都复核，先停止启动数据库。
        if cancel_requested():
            mgr.event("关闭已开始（复制完成），放弃启动数据库，仅做收尾")
            return
        missing = run.get("missing_segment") or ""
        restore_cmd = f"{RESTORE_SCRIPT} %f %p {missing or '-'}"
        if run.get("replay_delay_ms"):
            # 仅“重放中”关闭演示：逐段延迟重放，保证有关闭窗口
            restore_cmd += f" {int(run['replay_delay_ms'])}"
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

        # ---- 3. 启动前最后复核：准备中的任务不得继续启动数据库 ----
        demo_hold("handoff")   # 演示停点：紧贴启动交接处
        if cancel_requested():
            mgr.event("关闭已开始（配置就绪），不启动数据库，仅做收尾")
            return
        with mgr.lock:
            if mgr.closing:
                mgr.event("关闭已开始，不启动数据库，仅做收尾")
                return
            mgr.state = RECOVERING
        mgr.event("启动恢复实例，开始从 WAL 归档重放")
        r = start_postgres(run, pgdata, logfile)
        if r is None:
            mgr.event("关闭已开始，取消启动数据库，仅做收尾")
            return
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
        if cancel_requested():
            mgr.event("关闭已开始，停止重放轮询，等待恢复进程收尾")
            return
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
            if in_rec:
                # 演示停点：确实处于重放中（热备可读）
                demo_hold("replay", run.setdefault("_demo_once", {}))
            if not in_rec:
                # 已 promote —— 必须再核对日志中的目标落点证据
                # 关闭若已开始：迟到完成的实例不得开放结果
                if cancel_requested():
                    mgr.event("关闭已开始，实例虽已提升但不开放结果")
                    return
                with mgr.lock:
                    if mgr.closing:
                        mgr.event("关闭已开始，实例虽已提升但不开放结果")
                        return
                    mgr.state = VERIFYING
                mgr.event("实例已提升（promote），核对目标恢复点落点证据…")
                log = tail_file(logfile, n=400, max_bytes=524288)
                if target_re.search(log) and "archive recovery complete" in log:
                    # 关闭已开始：不抓取、不写回任何对外结果
                    if cancel_requested():
                        mgr.event("关闭已开始，结果不开放")
                        return
                    gather_results(run)
                    with mgr.lock:
                        # 关闭开始后不得写回 READY
                        if mgr.closing:
                            mgr.event("关闭已开始，结果不开放")
                            return
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
# 可控演示：在“复制中 / 启动交接处 / 重放中”人为停住，给运维窗口触发关闭。
# 仅用于演练，正常关闭为 off，不影响真实收尾逻辑。
# ---------------------------------------------------------------------------
STAGE_LABEL = {"copy": "复制中", "handoff": "启动交接处", "replay": "重放中"}


def demo_hold(stage: str, once: dict | None = None):
    if not mgr.demo_point or mgr.demo_point != stage:
        return
    if once is not None:
        if once.get(stage):
            return
        once[stage] = True
    mgr.event(
        f"[demo] 停在“{STAGE_LABEL.get(stage, stage)}”阶段，最多 {mgr.demo_delay:g}s；"
        "可在此窗口触发关闭，观察收尾行为"
    )
    # 可被关闭立即唤醒；超时自动继续，不影响正常演练
    mgr.cancel.wait(mgr.demo_delay)


# ---------------------------------------------------------------------------
# 正常关闭收尾：开始关闭后不再受理恢复；等待线程/启动命令结束；停止实例；
# 确认停止后才删除副本并完成关闭。迟到完成一律不开放结果、不写回 READY。
# ---------------------------------------------------------------------------
SHUTDOWN_CMD_GRACE = int(os.environ.get("SHUTDOWN_CMD_GRACE", "90"))
# 停止实例后用于确认“线程/命令已自行结束”的短暂复核窗口
SHUTDOWN_RECHECK_GRACE = int(os.environ.get("SHUTDOWN_RECHECK_GRACE", "10"))


def begin_shutdown() -> int:
    """进入关闭收尾。返回需要等待的恢复线程数。"""
    with mgr.lock:
        if mgr.closing:
            return len(mgr.threads)
        mgr.closing = True
        mgr.cancel.set()
        if mgr.state not in (SHUTTING_DOWN, STOPPING):
            mgr.state = SHUTTING_DOWN
        n = len(mgr.threads)
    print(f"[shutdown] 开始关闭收尾：不再受理恢复，在途任务 {n} 个", flush=True)
    return n


def _thread_alive(run_id: str) -> bool:
    with mgr.lock:
        th = mgr.threads.get(run_id)
    return bool(th and th.is_alive())


def wait_recovery_threads(grace: int) -> list[str]:
    deadline = time.monotonic() + grace
    while True:
        with mgr.lock:
            threads = list(mgr.threads.items())
        alive = [(rid, th) for rid, th in threads if th.is_alive()]
        if not alive:
            return []
        if time.monotonic() >= deadline:
            for rid, _ in alive:
                print(f"[shutdown] 等待恢复线程超时：{rid}", flush=True)
            return [rid for rid, _ in alive]
        time.sleep(0.2)


def wait_start_commands(grace: int) -> list[str]:
    """等待本次启动/停止命令（pg_ctl start/stop）全部结束。"""
    deadline = time.monotonic() + grace
    while True:
        with mgr.lock:
            procs = dict(mgr.child_procs)
        if not procs:
            return []
        if time.monotonic() >= deadline:
            return list(procs)
        time.sleep(0.2)


def discover_run_pgdata() -> dict[str, Path]:
    """扫描运行目录，补齐尚未登记/迟到创建的恢复实例（与启动命令交错时）。"""
    found = {}
    try:
        for d in RUNS_DIR.glob("run_*"):
            pgdata = d / "pgdata"
            if pgdata.exists():
                found[d.name] = pgdata
    except OSError:
        pass
    return found


def stop_all_instances(timeout: int | None = None) -> list[str]:
    """停止所有恢复数据库并确认停止（含与启动命令交错时迟到创建的实例）。

    以截止时间而非固定轮数判定，确保启动中的实例也有足够的停止窗口。
    """
    deadline = time.monotonic() + (timeout if timeout is not None else 30)
    while True:
        with mgr.lock:
            dirs = dict(mgr.active_pgdata)
            dirs.update({k: v for k, v in discover_run_pgdata().items() if k not in dirs})
        alive = {rid: d for rid, d in dirs.items() if postmaster_alive(d)}
        if not alive:
            return []
        for rid, pgdata in alive.items():
            print(f"[shutdown] 停止恢复实例 {rid}（{pgdata}）", flush=True)
            stop_postgres(pgdata, run_id=rid)
        if time.monotonic() >= deadline:
            break
        time.sleep(0.3)
    with mgr.lock:
        dirs = dict(mgr.active_pgdata)
        dirs.update({k: v for k, v in discover_run_pgdata().items() if k not in dirs})
    return [rid for rid, d in dirs.items() if postmaster_alive(d)]


def remove_replica(run_id: str, pgdata: Path):
    """删除该任务副本目录（仅在其数据库确认停止后调用）。"""
    with mgr.lock:
        if postmaster_alive(pgdata):
            print(f"[shutdown] 实例仍未停止，保留副本 {pgdata}", flush=True)
            return False
    shutil.rmtree(pgdata.parent, ignore_errors=True)
    return True


def finish_shutdown() -> bool:
    """完成关闭收尾；仅在全部结束且实例确认停止后删除副本。返回是否干净完成。

    顺序很重要：
      1) 先停止“已存在”的恢复实例（可让卡在 `pg_ctl -w start` 的命令尽快返回）；
      2) 等本次启动/停止命令全部结束；
      3) 等恢复线程全部结束；
      4) 再停止一次——与启动命令交错、直到此时才创建出来的迟到实例也必须停止；
      5) 全部确认后才删除副本。
    """
    with mgr.lock:
        mgr.state = STOPPING

    alive = stop_all_instances()
    cmds = wait_start_commands(SHUTDOWN_CMD_GRACE)
    run_ids = wait_recovery_threads(SHUTDOWN_THREAD_GRACE)
    alive = stop_all_instances()
    # 停止实例会让仍在收尾的线程/启动命令迅速结束：再做一轮短暂确认，
    # 但绝不因等待超时而提前删除副本
    if run_ids or cmds:
        cmds = wait_start_commands(SHUTDOWN_RECHECK_GRACE)
        run_ids = wait_recovery_threads(SHUTDOWN_RECHECK_GRACE)
        alive = stop_all_instances(timeout=SHUTDOWN_RECHECK_GRACE)
    run_ids = [r for r in run_ids if _thread_alive(r)]

    clean = not run_ids and not cmds and not alive
    removed, kept = [], []
    if clean:
        # 恢复线程、启动命令全部结束且实例确认停止 —— 才允许删除副本
        with mgr.lock:
            dirs = dict(mgr.active_pgdata)
            dirs.update({k: v for k, v in discover_run_pgdata().items() if k not in dirs})
        for rid, pgdata in dirs.items():
            (removed if remove_replica(rid, pgdata) else kept).append(rid)
        if not kept:
            # 此时所有恢复实例均已确认停止：连带清理未登记的残留运行目录
            try:
                for d in RUNS_DIR.glob("run_*"):
                    shutil.rmtree(d, ignore_errors=True)
            except OSError:
                pass
        clean = not kept
    with mgr.lock:
        if clean:
            mgr.active_pgdata.clear()
            mgr.threads.clear()
            mgr.child_procs.clear()
            mgr.run = None
            mgr.state = IDLE
    if not clean:
        print(
            "[shutdown] 未能干净收尾："
            f"线程未结束 {run_ids or '无'}；启动命令未结束 {cmds or '无'}；"
            f"实例未停止 {alive or kept or '无'}。保留副本供下次启动清理，不伪造完成。",
            flush=True,
        )
    else:
        print(
            f"[shutdown] 恢复线程与启动命令已结束、恢复实例已确认停止；"
            f"已删除副本 {removed or '无'}，关闭完成",
            flush=True,
        )
    return clean


# ---------------------------------------------------------------------------
# 应用生命周期
#   启动：清理上一轮遗留的恢复实例（含迟到创建的目录），保证“全站同时只恢复一份”
#   关闭：先拒绝新恢复并通知在途任务停止；等恢复线程、本次启动命令、恢复实例
#         全部结束且确认停止后才删除副本；结束前绝不把迟到结果开放为 READY
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
    # 正常关闭收尾：uvicorn 收到 SIGTERM 后先停受理请求，再进入此处。
    # begin_shutdown() 置位关闭标记，任何在途恢复线程都会在下一个检查点停止，
    # 不再启动数据库；finish_shutdown() 等待线程/启动命令结束、实例确认停止后
    # 才删除副本。迟到完成的任务不开放结果、不写回 READY。
    begin_shutdown()
    finish_shutdown()


app = FastAPI(title="遥测恢复演练台", lifespan=lifespan,
              docs_url=None, redoc_url=None, openapi_url=None)


class RecoverRequest(BaseModel):
    restore_point: str
    inject_fault: bool = False
    close_demo: str | None = None   # 仅演练：copy / handoff / replay 停点


@app.get("/", include_in_schema=False)
def index():
    return FileResponse(STATIC_DIR / "index.html")


@app.get("/api/health")
def api_health():
    with mgr.lock:
        return {"ok": True, "closing": mgr.closing, "state": mgr.state}


@app.get("/api/demo")
def api_demo():
    """当前可控演示停点（空表示正常关闭，无演示）。"""
    with mgr.lock:
        return {"close_point": mgr.demo_point or None, "delay_sec": mgr.demo_delay}


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
        if mgr.closing:
            raise HTTPException(409, "应用正在关闭，不再受理恢复任务")
        if mgr.state != IDLE:
            raise HTTPException(
                409,
                f"当前状态为 {mgr.state}：全站同时只允许一份恢复。"
                + ("请先回收失败残库。" if mgr.state == FAILED else "请先回收当前恢复库。"),
            )
        demo = (req.close_demo or "").strip().lower() or None
        if demo is not None and demo not in STAGE_LABEL:
            raise HTTPException(400, f"未知关闭演示停点: {demo}")
        if demo is not None:
            # 演示停点属于本次恢复任务，回收后自动失效
            mgr.demo_point = demo
            mgr.demo_delay = DEMO_CLOSE_DELAY or 60.0
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
            "closing": False,
            # 仅“重放中”关闭演示需要拉长重放窗口；其它停点不影响正常重放速度
            "replay_delay_ms": (REPLAY_DEMO_DELAY_MS if demo == "replay" else 0),
        }
        mgr.state = PREPARING
        th = threading.Thread(target=do_recovery, args=(mgr.run,), daemon=True,
                              name=f"recovery-{run_id}")
        mgr.threads[run_id] = th
        mgr.event(
            f"创建恢复任务 {run_id}，目标恢复点 {req.restore_point}"
            + ("（注入归档缺段故障）" if req.inject_fault else "")
        )
    th.start()
    return {"ok": True, "run_id": run_id}


@app.get("/api/status")
def api_status():
    with mgr.lock:
        state = mgr.state
        run = ({k: v for k, v in mgr.run.items() if not k.startswith("_")}
               if mgr.run else None)
        closing = mgr.closing
    log_tail = ""
    if run and not closing:
        log_tail = tail_file(Path(run["rundir"]) / "postgres.log", 80)
    return {"state": state, "run": run, "log_tail": log_tail, "closing": closing}


@app.post("/api/reclaim")
def api_reclaim():
    """回收：停止恢复实例并删除残库。READY/FAILED 状态下调用后回到 IDLE 才允许再次恢复。"""
    with mgr.lock:
        if mgr.closing:
            raise HTTPException(409, "应用正在关闭，不再受理回收操作")
        if mgr.state not in (READY, FAILED):
            raise HTTPException(409, f"当前状态 {mgr.state} 无需回收")
        run = mgr.run
    stop_postgres(Path(run["rundir"]) / "pgdata", run_id=run["id"])
    shutil.rmtree(run["rundir"], ignore_errors=True)
    with mgr.lock:
        mgr.state = IDLE
        mgr.run = None
        mgr.demo_point = DEMO_CLOSE_POINT or None
        mgr.demo_delay = DEMO_CLOSE_DELAY
        mgr.threads.pop(run["id"], None)
        mgr.active_pgdata.pop(run["id"], None)
    return {"ok": True}


def require_ready() -> dict:
    """结果开放门禁：仅在已确认到达目标恢复点（READY）时放行；失败残库一律拒绝。"""
    with mgr.lock:
        if (mgr.closing or mgr.state != READY or mgr.run is None
                or mgr.run.get("result") is None):
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
