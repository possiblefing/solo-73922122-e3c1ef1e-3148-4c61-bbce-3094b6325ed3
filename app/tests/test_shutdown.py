"""关闭收尾回归测试（不需要真实 PostgreSQL / Docker）。

运行：python3 -I -m unittest discover -s app/tests -t . -v
说明：-I 隔离模式，避免从测试/数据目录加载同名模块。
"""
import importlib
import os
import sys
import threading
import time
import types
import unittest
from pathlib import Path
from unittest import mock

# ---------------------------------------------------------------------------
# 依赖桩：环境内没有 fastapi/psycopg，用最小桩替换以便导入 main 并直接测状态机
# ---------------------------------------------------------------------------
try:  # 若真实依赖可用则直接使用
    import fastapi  # noqa: F401
    import psycopg  # noqa: F401
except Exception:  # pragma: no cover - 仅在缺依赖时为测试提供桩
    fake = types.ModuleType("fastapi")

    class _HTTPException(Exception):
        def __init__(self, status_code, detail=None):
            self.status_code = status_code
            self.detail = detail

    class _FastAPI:
        def __init__(self, *a, **kw):
            pass

        def get(self, *a, **kw):
            return lambda fn: fn

        post = get

    fake.FastAPI = _FastAPI
    fake.HTTPException = _HTTPException
    fake.Query = lambda *a, **kw: None
    fake.responses = types.ModuleType("fastapi.responses")
    fake.responses.FileResponse = lambda *a, **kw: None
    sys.modules["fastapi"] = fake
    sys.modules["fastapi.responses"] = fake.responses

    rows = types.ModuleType("psycopg.rows")
    rows.dict_row = object()
    psy = types.ModuleType("psycopg")
    psy.OperationalError = type("OperationalError", (Exception,), {})
    psy.connect = lambda *a, **kw: (_ for _ in ()).throw(psy.OperationalError("no db"))
    psy.rows = rows
    sys.modules["psycopg"] = psy
    sys.modules["psycopg.rows"] = rows

    pyd = types.ModuleType("pydantic")

    class _BaseModel:
        def __init__(self, **kw):
            self.__dict__.update(kw)

    pyd.BaseModel = _BaseModel
    sys.modules["pydantic"] = pyd

APP_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(APP_DIR))
main = importlib.import_module("main")


class FakeProc:
    def __init__(self, returncode=0):
        self.returncode = returncode


class ShutdownTests(unittest.TestCase):
    def setUp(self):
        self.tmp = Path(self._tmpdir())
        self.mgr = main.mgr
        with self.mgr.lock:
            self.mgr.state = main.IDLE
            self.mgr.run = None
            self.mgr.closing = False
            self.mgr.cancel.clear()
            self.mgr.threads.clear()
            self.mgr.child_procs.clear()
            self.mgr.active_pgdata.clear()
        self.mgr.demo_point = None

    def _tmpdir(self):
        import tempfile
        return tempfile.mkdtemp(prefix="trd-test-")

    def _run_ctx(self, rundir: Path):
        return {
            "id": rundir.name,
            "restore_point": "rp_before_delete",
            "inject_fault": False,
            "missing_segment": None,
            "started_at": main.now_iso(),
            "finished_at": None,
            "rundir": str(rundir),
            "events": [],
            "replay_lsn": None,
            "replay_ts": None,
            "error": None,
            "result": None,
        }

    # ---- 1. 开始关闭后不接受新恢复 --------------------------------------
    def test_recover_rejected_after_closing(self):
        with self.mgr.lock:
            self.mgr.run = None
        with mock.patch.object(main, "load_manifest", return_value={
                "restore_points": [{"name": "rp_before_delete"}],
                "first_wal_after_backup": "x"}):
            with mock.patch.object(main, "BASEBACKUP_DIR", self.tmp), \
                 mock.patch.object(main, "RUNS_DIR", self.tmp):
                main.begin_shutdown()
                with self.assertRaises(main.HTTPException) as ctx:
                    main.api_recover(main.RecoverRequest(restore_point="rp_before_delete"))
                self.assertEqual(ctx.exception.status_code, 409)
                self.assertIn("关闭", ctx.exception.detail)

    # ---- 2. 准备中的任务不得继续启动数据库 ------------------------------
    def test_preparing_task_does_not_start_database(self):
        rundir = self.tmp / "run_x"
        run = self._run_ctx(rundir)
        # 模拟：复制完成后关闭已开始
        with mock.patch.object(main, "copy_basebackup",
                               side_effect=lambda r, s, d: (d.mkdir(parents=True), False)[1]), \
             mock.patch.object(main, "start_postgres") as start, \
             mock.patch.object(main, "stop_postgres") as stop:
            main.begin_shutdown()
            main.do_recovery(run)
        start.assert_not_called()
        stop.assert_not_called()
        self.assertTrue(self.mgr.closing)
        self.assertNotEqual(self.mgr.state, main.RECOVERING)

    # ---- 3. 启动交接处：取消后不启动 ------------------------------------
    def test_handoff_cancel_skips_start(self):
        rundir = self.tmp / "run_y"
        run = self._run_ctx(rundir)
        self.mgr.run = run   # event() 写入当前任务事件流
        data = self.tmp / "base"
        data.mkdir()

        def copy_then_cancel(r, src, dst):
            dst.mkdir(parents=True)
            main.begin_shutdown()   # 复制完成、配置前关闭
            return True

        with mock.patch.object(main, "BASEBACKUP_DIR", data), \
             mock.patch.object(main, "copy_basebackup", side_effect=copy_then_cancel), \
             mock.patch.object(main, "start_postgres") as start:
            main.do_recovery(run)
        start.assert_not_called()
        self.assertIn("关闭已开始", " ".join(e["msg"] for e in run["events"]))

    # ---- 4. 迟到完成的实例不得写回 READY / 开放结果 ----------------------
    def test_late_completion_does_not_write_ready(self):
        rundir = self.tmp / "run_z"
        run = self._run_ctx(rundir)
        pgdata = rundir / "pgdata"
        logdir = rundir
        rundir.mkdir()
        log = rundir / "postgres.log"
        log.write_text(
            'LOG: recovery stopping at restore point "rp_before_delete"\n'
            "LOG: archive recovery complete\n", encoding="utf-8")
        with mock.patch.object(main, "recovered_dsn", return_value=""), \
             mock.patch.object(main.psycopg, "connect") as connect, \
             mock.patch.object(main, "gather_results") as gather, \
             mock.patch.object(main, "run_cmd", return_value=FakeProc(0)), \
             mock.patch.object(main, "RECOVERY_TIMEOUT", 5):
            conn = mock.MagicMock()
            conn.execute.return_value.fetchone.return_value = (False, "0/1", None)
            connect.return_value.__enter__.return_value = conn
            main.begin_shutdown()
            main.poll_until_ready(run, pgdata, log, "rp_before_delete")
        gather.assert_not_called()
        self.assertNotEqual(self.mgr.state, main.READY)
        self.assertIsNone(run["result"])

    # ---- 5. 线程/命令未结束、实例未停止时不得删除副本 --------------------
    def test_no_replica_delete_until_clean(self):
        rundir = self.tmp / "run_keep"
        rundir.mkdir()
        (rundir / "pgdata").mkdir()
        rel = threading.Event()

        def stuck():
            rel.wait(30)

        th = threading.Thread(target=stuck)
        th.start()
        with self.mgr.lock:
            self.mgr.threads[rundir.name] = th
            self.mgr.active_pgdata[rundir.name] = rundir / "pgdata"
        with mock.patch.object(main, "SHUTDOWN_THREAD_GRACE", 0), \
             mock.patch.object(main, "SHUTDOWN_RECHECK_GRACE", 0), \
             mock.patch.object(main, "wait_start_commands", return_value=[]), \
             mock.patch.object(main, "stop_all_instances", return_value=[]):
            clean = main.finish_shutdown()
        self.assertFalse(clean)
        self.assertTrue((rundir / "pgdata").exists(), "线程仍在运行时不得删除副本")
        rel.set()
        th.join(5)

    # ---- 6. 干净收尾后删除副本并回到 IDLE --------------------------------
    def test_clean_shutdown_removes_replica(self):
        rundir = self.tmp / "run_ok"
        (rundir / "pgdata").mkdir(parents=True)
        with self.mgr.lock:
            self.mgr.active_pgdata[rundir.name] = rundir / "pgdata"
        with mock.patch.object(main, "SHUTDOWN_THREAD_GRACE", 0), \
             mock.patch.object(main, "wait_start_commands", return_value=[]), \
             mock.patch.object(main, "postmaster_alive", return_value=False), \
             mock.patch.object(main, "discover_run_pgdata", return_value={}):
            clean = main.finish_shutdown()
        self.assertTrue(clean)
        self.assertFalse(rundir.exists())
        self.assertEqual(self.mgr.state, main.IDLE)
        self.assertFalse(self.mgr.closing)

    # ---- 7. 启动命令未结束（与启动交错）时保留副本 ------------------------
    def test_lingering_start_command_blocks_delete(self):
        rundir = self.tmp / "run_cmd"
        (rundir / "pgdata").mkdir(parents=True)
        with self.mgr.lock:
            self.mgr.active_pgdata[rundir.name] = rundir / "pgdata"
        with mock.patch.object(main, "SHUTDOWN_THREAD_GRACE", 0), \
             mock.patch.object(main, "wait_start_commands", return_value=["pg_ctl start"]), \
             mock.patch.object(main, "postmaster_alive", return_value=False):
            clean = main.finish_shutdown()
        self.assertFalse(clean)
        self.assertTrue((rundir / "pgdata").exists())

    # ---- 8. stop_all_instances 发现迟到创建的实例 ------------------------
    def test_stop_all_finds_late_instance(self):
        late = self.tmp / "run_late" / "pgdata"
        late.mkdir(parents=True)
        calls = []

        def fake_stop(pgdata, run_id=None):
            calls.append(str(pgdata))
            return True

        # 第一次发现“活着”，停止后确认停止
        states = iter([True, False])
        with mock.patch.object(main, "discover_run_pgdata", return_value={"run_late": late}), \
             mock.patch.object(main, "postmaster_alive", side_effect=lambda d: next(states, False)), \
             mock.patch.object(main, "stop_postgres", side_effect=fake_stop):
            alive = main.stop_all_instances(timeout=1)
        self.assertEqual(alive, [])
        self.assertEqual(calls, [str(late)])


class CopyDemoTests(unittest.TestCase):
    def setUp(self):
        import tempfile
        self.tmp = Path(tempfile.mkdtemp(prefix="trd-copy-"))
        self.mgr = main.mgr
        with self.mgr.lock:
            self.mgr.state = main.IDLE
            self.mgr.run = None
            self.mgr.closing = False
            self.mgr.cancel.clear()
            self.mgr.threads.clear()
            self.mgr.child_procs.clear()
            self.mgr.active_pgdata.clear()

    def _run_ctx(self, rundir: Path):
        return {
            "id": rundir.name, "restore_point": "rp_before_delete",
            "inject_fault": False, "missing_segment": None,
            "started_at": main.now_iso(), "finished_at": None,
            "rundir": str(rundir), "events": [], "replay_lsn": None,
            "replay_ts": None, "error": None, "result": None,
        }

    def test_copy_aborts_when_cancelled(self):
        tmp = self.tmp
        src = tmp / "src"
        src.mkdir()
        for i in range(5):
            (src / f"f{i}").write_bytes(b"x" * 10)
        dst = tmp / "dst"
        mgr = main.mgr
        mgr.cancel.clear()
        rundir = tmp / "run_c"
        run = {"id": "run_c", "rundir": str(rundir), "events": []}
        with mock.patch.object(main, "demo_hold", side_effect=lambda *a, **k: mgr.cancel.set()):
            ok = main.copy_basebackup(run, src, dst)
        self.assertFalse(ok, "关闭开始后复制应中止，不留下可启动的半成品")


    # ---- 9. 启动命令执行期间关闭：登记可见、结束后摘除 --------------------
    def test_start_command_registered_during_startup(self):
        rundir = self.tmp / "run_reg"
        run = self._run_ctx(rundir)
        seen = {}

        created = {}

        class FakePopen:
            def __init__(self, args, **kw):
                self.args = args
                self.returncode = 0

            def communicate(self, timeout=None):
                # 此时 start_postgres 已释放锁、命令仍在执行中
                with self_mgr.lock:
                    created["during"] = dict(self_mgr.child_procs)
                return ("", "")

            def kill(self):
                pass

        self_mgr = self.mgr

        with mock.patch.object(main, "cancel_requested", return_value=False), \
             mock.patch.object(main, "claim_pgdata", return_value=True), \
             mock.patch.object(main, "copy_basebackup",
                               side_effect=lambda r, s, d: (d.mkdir(parents=True), True)[1]), \
             mock.patch.object(main, "demo_hold"), \
             mock.patch.object(main, "run_cmd", return_value=FakeProc(0)), \
             mock.patch.object(main.subprocess, "Popen", FakePopen), \
             mock.patch.object(main, "poll_until_ready"):
            main._do_recovery(run)
        self.assertIn(run["id"], created.get("during", {}),
                      "启动命令执行期间必须可被关闭流程看到")
        with self.mgr.lock:
            self.assertNotIn(run["id"], self.mgr.child_procs)

    # ---- 10. start_postgres 与关闭在同一把锁下判定 -----------------------
    def test_start_postgres_refuses_after_closing(self):
        rundir = self.tmp / "run_refuse"
        rundir.mkdir()
        run = self._run_ctx(rundir)
        with mock.patch.object(main.subprocess, "Popen") as popen:
            main.begin_shutdown()
            self.assertIsNone(main.start_postgres(run, rundir, rundir / "log"))
        popen.assert_not_called()

    # ---- 11. 交错的启动实例：关闭停止它并确认后才删除 --------------------
    def test_interleaved_instance_is_stopped_then_removed(self):
        rundir = self.tmp / "run_inter"
        pgdata = rundir / "pgdata"
        pgdata.mkdir(parents=True)
        with self.mgr.lock:
            self.mgr.active_pgdata[rundir.name] = pgdata
        stopping = {"stopped": False}

        def fake_stop(pgdata_path, run_id=None):
            stopping["stopped"] = True
            return True

        with mock.patch.object(main, "SHUTDOWN_THREAD_GRACE", 0), \
             mock.patch.object(main, "wait_start_commands", return_value=[]), \
             mock.patch.object(main, "discover_run_pgdata", return_value={}), \
             mock.patch.object(main, "postmaster_alive",
                               side_effect=lambda d: not stopping["stopped"]), \
             mock.patch.object(main, "stop_postgres", side_effect=fake_stop):
            clean = main.finish_shutdown()
        self.assertTrue(stopping["stopped"], "交错的恢复实例必须先被停止")
        self.assertTrue(clean)
        self.assertFalse(rundir.exists(), "确认停止后才删除副本")


if __name__ == "__main__":
    unittest.main()
