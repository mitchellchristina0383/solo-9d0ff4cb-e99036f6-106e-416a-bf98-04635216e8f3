"""
lock-lab —— 库位调拨行锁互斥实验（Flask + PostgreSQL）

两笔调拨单各开一个独立连接/事务，对同两条 inventory 记录执行
SELECT ... FOR UPDATE。可选择：
  * same     相同加锁次序：两笔都按 location_id 升序加锁 -> 普通等待，不会死锁
  * opposite 相反加锁次序：T1 先 A-01 后 A-02，T2 先 A-02 后 A-01 -> 数据库可报死锁

死锁只认 PostgreSQL 返回的 SQLSTATE 40P01，不自行推断。
"""

import logging
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import psycopg2
from flask import Flask, jsonify, render_template, request

from wait_snapshot import fetch_inventory, fetch_lock_waits

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
log = logging.getLogger("lock-lab")

app = Flask(__name__)

DB_CONF = {
    "host": os.environ.get("PGHOST", "db"),
    "port": int(os.environ.get("PGPORT", "5432")),
    "dbname": os.environ.get("PGDATABASE", "locklab"),
    "user": os.environ.get("PGUSER", "locklab"),
    "password": os.environ.get("PGPASSWORD", "locklab"),
}

TRANSFERS = {
    "T1": {"id": "TR-1001", "from": "A-01", "to": "A-02", "qty": 10},
    "T2": {"id": "TR-1002", "from": "A-02", "to": "A-01", "qty": 10},
}
ORDERED_LOCATIONS = ["A-01", "A-02"]

# 快照最多保留多少条，避免长时间等待时内存无限增长
MAX_SNAPSHOTS = 600
# 等待关系采样间隔（秒）
SAMPLE_INTERVAL = 0.1
# 历史轮次最多保留多少轮
MAX_HISTORY = 50

# 全局只允许一轮在跑（等待关系必须来自同一批连接才好复盘）
_manager_lock = threading.Lock()
_active_run = None          # type: ignore[var-annotated]
_runs: dict[str, "Run"] = {}


def now_iso():
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


def connect(app_name: str):
    conn = psycopg2.connect(application_name=app_name, **DB_CONF)
    conn.autocommit = False
    return conn


# --------------------------------------------------------------------------- #
# 一轮实验的状态容器
# --------------------------------------------------------------------------- #
@dataclass
class Run:
    run_id: str
    lock_order: str
    lock_timeout_ms: int
    deadlock_timeout_ms: int
    stagger_ms: int
    hard_timeout_s: float
    status: str = "running"                 # running / finished / cancelled
    error: str | None = None
    created_at: str = field(default_factory=now_iso)
    finished_at: str | None = None
    start_ts: float = 0.0
    snapshots: list[dict] = field(default_factory=list)
    workers: dict = field(default_factory=dict)
    cancel_event: threading.Event = field(default_factory=threading.Event)
    conns: dict = field(default_factory=dict)
    lock: threading.Lock = field(default_factory=threading.Lock)
    inventory_after: list[dict] | None = None
    wait_cycle: list[str] | None = None
    deadlock_victim: str | None = None
    db_declared_deadlock: bool = False

    def public(self):
        with self.lock:
            return {
                "run_id": self.run_id,
                "lock_order": self.lock_order,
                "lock_timeout_ms": self.lock_timeout_ms,
                "deadlock_timeout_ms": self.deadlock_timeout_ms,
                "stagger_ms": self.stagger_ms,
                "hard_timeout_s": self.hard_timeout_s,
                "status": self.status,
                "error": self.error,
                "created_at": self.created_at,
                "finished_at": self.finished_at,
                "snapshots": self.snapshots,
                "workers": self.workers,
                "inventory_after": self.inventory_after,
                "wait_cycle": self.wait_cycle,
                "deadlock_victim": self.deadlock_victim,
                "db_declared_deadlock": self.db_declared_deadlock,
                "transfers": TRANSFERS,
            }

    def add_snapshot(self, snap: dict):
        with self.lock:
            if len(self.snapshots) < MAX_SNAPSHOTS:
                self.snapshots.append(snap)

    def update_worker(self, key: str, patch: dict):
        with self.lock:
            self.workers.setdefault(key, {})
            self.workers[key].update(patch)


# --------------------------------------------------------------------------- #
# 单笔调拨工作线程
# --------------------------------------------------------------------------- #
def lock_sequence(key: str, lock_order: str) -> list[str]:
    """返回该调拨单的实际加锁顺序。"""
    if lock_order == "same":
        # 两笔都按统一的库位顺序加锁 -> 不会形成等待环
        return list(ORDERED_LOCATIONS)
    # opposite：按调拨方向“先锁源库位、再锁目标库位”，两笔方向相反
    t = TRANSFERS[key]
    return [t["from"], t["to"]]


def worker(run: Run, key: str, barrier: threading.Barrier):
    """
    每个调拨单一条独立数据库连接、一个事务：
      BEGIN -> 逐行 SELECT ... FOR UPDATE -> UPDATE 两条库存 -> COMMIT
    """
    t = TRANSFERS[key]
    app_name = f"locklab-{key}"
    state = {
        "key": key,
        "transfer_id": t["id"],
        "app_name": app_name,
        "pid": None,
        "phase": "connecting",
        "locks_acquired": [],
        "result": None,          # committed / rolled_back
        "sqlstate": None,
        "message": None,
        "timeline": [],
    }
    run.update_worker(key, state)

    def note(phase: str):
        state["phase"] = phase
        state["timeline"].append({"t": round(time.time() - run.start_ts, 3), "phase": phase})
        run.update_worker(key, state)

    conn = None
    try:
        conn = connect(app_name)
        run.conns[key] = conn
        with conn.cursor() as cur:
            cur.execute("SELECT pg_backend_pid()")
            state["pid"] = cur.fetchone()[0]
            run.update_worker(key, state)

            cur.execute("SET lock_timeout = %s", (run.lock_timeout_ms,))
            cur.execute("SET deadlock_timeout = %s", (run.deadlock_timeout_ms,))
            note("begin")
            cur.execute("BEGIN")

            seq = lock_sequence(key, run.lock_order)
            state["lock_sequence"] = seq
            run.update_worker(key, state)

            # 三方屏障：两个工作线程与监控线程在此同时起跑，
            # 保证从“首锁请求”那一刻起监控就在采样等待关系
            note("ready_waiting_barrier")
            barrier.wait()

            for idx, loc in enumerate(seq):
                if run.cancel_event.is_set():
                    raise RuntimeError("run cancelled before locking %s" % loc)
                note("requesting_lock:" + loc)
                cur.execute(
                    "SELECT qty_on_hand FROM inventory "
                    "WHERE location_id = %s FOR UPDATE",
                    (loc,),
                )
                cur.fetchone()
                note("lock_acquired:" + loc)
                state["locks_acquired"].append(loc)
                run.update_worker(key, state)

                # 拿到第一把锁后停顿，保证两笔都先持有一把再去抢对方那把，
                # 相反次序下等待环稳定出现；相同次序下表现为排队等待
                if idx == 0 and not run.cancel_event.is_set():
                    if run.cancel_event.wait(run.stagger_ms / 1000.0):
                        raise RuntimeError("run cancelled during stagger")

            note("updating")
            cur.execute(
                "UPDATE inventory SET qty_on_hand = qty_on_hand - %s, "
                "updated_at = now() WHERE location_id = %s",
                (t["qty"], t["from"]),
            )
            cur.execute(
                "UPDATE inventory SET qty_on_hand = qty_on_hand + %s, "
                "updated_at = now() WHERE location_id = %s",
                (t["qty"], t["to"]),
            )
            note("committing")
            conn.commit()
            state["result"] = "committed"
            state["message"] = "事务已提交"
            note("committed")
    except threading.BrokenBarrierError:
        # 屏障在异常路径下被重置时，工作线程直接退出即可
        log.warning("worker %s barrier broken, exiting", key)
        state["result"] = "rolled_back"
        state["message"] = "同步屏障异常，工作线程退出"
    except Exception as exc:  # noqa: BLE001 - 需把所有结局如实回传页面
        sqlstate = getattr(exc, "pgcode", None)
        state["sqlstate"] = sqlstate
        if conn is not None:
            try:
                conn.rollback()
            except Exception:  # pragma: no cover
                pass
        if sqlstate == "40P01":
            # 唯一的死锁判定来源：数据库自己报 deadlock detected
            state["result"] = "rolled_back"
            state["message"] = "PostgreSQL 检测到死锁，本事务被数据库回滚（40P01）"
            note("deadlock_rollback")
            run.db_declared_deadlock = True
            run.deadlock_victim = key
        elif sqlstate == "55P03":
            state["result"] = "rolled_back"
            state["message"] = "等待行锁超过 lock_timeout，本事务回滚（55P03）"
            note("lock_timeout_rollback")
        else:
            state["result"] = "rolled_back"
            state["message"] = f"{type(exc).__name__}: {exc}"
            note("error_rollback")
        log.warning("worker %s ended: %s", key, state["message"])
    finally:
        state["phase"] = "done"
        run.update_worker(key, state)
        # 无论成功失败，本轮连接必须释放回操作系统
        if conn is not None:
            try:
                conn.close()
            except Exception:  # pragma: no cover
                pass
            run.conns.pop(key, None)


# --------------------------------------------------------------------------- #
# 监控线程：周期读取数据库里真实的等待关系
# --------------------------------------------------------------------------- #
def monitor(run: Run, barrier: threading.Barrier):
    try:
        barrier.wait()
    except threading.BrokenBarrierError:
        log.warning("monitor barrier broken, exiting")
        return
    admin = connect("locklab-monitor")
    sample_no = 0
    try:
        while True:
            time.sleep(SAMPLE_INTERVAL)
            sample_no += 1
            try:
                edges = fetch_lock_waits(admin)
                _annotate_waited_locations(run, edges)
            except Exception as exc:  # noqa: BLE001
                log.warning("snapshot failed: %s", exc)
                edges = []
            snap = {
                "t": round(time.time() - run.start_ts, 3),
                "sample": sample_no,
                "edges": edges,
            }
            run.add_snapshot(snap)

            # 硬性兜底超时：取消并强制 terminate 本轮连接，避免遗留
            if time.time() - run.start_ts > run.hard_timeout_s:
                run.error = (
                    f"硬性超时 {run.hard_timeout_s:.0f}s 到达，"
                    "已取消本轮并强制释放数据库连接"
                )
                run.cancel_event.set()
                _terminate_run_connections(run, admin)
                break

            if all(w.get("phase") == "done" for w in run.workers.values()) \
                    and len(run.workers) == 2:
                break
    finally:
        try:
            admin.close()
        except Exception:  # pragma: no cover
            pass


# 各工作线程“当前正在请求的库位”，用于在数据库视图未带 tuple 信息时补注
# （PG15 行锁等待者只有 transactionid/ShareLock，拿不到具体行）
def _annotate_waited_locations(run: Run, edges: list[dict]):
    for e in edges:
        if e.get("waited_location"):
            continue
        app = e.get("blocked_app", "")
        key = app.replace("locklab-", "")
        w = run.workers.get(key)
        if w and w.get("phase", "").startswith("requesting_lock:"):
            e["waited_location"] = w["phase"].split(":", 1)[1]
            e["location_source"] = "app-annotation"


def _terminate_run_connections(run: Run, admin_conn):
    """超时兜底：按 pid 终止本轮两个后端，确保连接被释放。"""
    with admin_conn.cursor() as cur:
        for key in list(run.conns.keys()):
            conn = run.conns.get(key)
            pid = run.workers.get(key, {}).get("pid")
            if pid:
                cur.execute("SELECT pg_terminate_backend(%s)", (pid,))
                log.warning("hard timeout: terminated backend pid=%s (%s)", pid, key)
            if conn is not None:
                try:
                    conn.close()
                except Exception:  # pragma: no cover
                    pass
            run.conns.pop(key, None)
    admin_conn.commit()


# --------------------------------------------------------------------------- #
# 一轮实验的编排
# --------------------------------------------------------------------------- #
def reset_inventory():
    conn = connect("locklab-reset")
    try:
        with conn.cursor() as cur:
            cur.execute(
                "UPDATE inventory SET qty_on_hand = 100, updated_at = now() "
                "WHERE location_id IN ('A-01','A-02')"
            )
        conn.commit()
    finally:
        conn.close()


def derive_wait_cycle(run: Run):
    """从数据库观测到的等待边里找环（仅用于页面高亮，死锁判定仍只看 40P01）。"""
    graph: dict[str, set[str]] = {}
    for snap in run.snapshots:
        for e in snap["edges"]:
            graph.setdefault(e["blocked_app"], set()).add(e["blocking_app"])
    for start in graph:
        stack = [(start, [start])]
        while stack:
            node, path = stack.pop()
            for nxt in graph.get(node, ()):  # type: ignore[union-attr]
                if nxt == start and len(path) >= 2:
                    return path + [start]
                if nxt not in path:
                    stack.append((nxt, path + [nxt]))
    return None


def run_supervisor(run: Run):
    global _active_run
    try:
        reset_inventory()
        run.start_ts = time.time()
        barrier = threading.Barrier(3)
        threads = [
            threading.Thread(target=worker, args=(run, "T1", barrier), name="T1"),
            threading.Thread(target=worker, args=(run, "T2", barrier), name="T2"),
            threading.Thread(target=monitor, args=(run, barrier), name="monitor"),
        ]
        for th in threads:
            th.start()
        for th in threads:
            th.join()

        # 监控线程可能因硬超时提前退出；兜底再收一次
        run.cancel_event.set()

        admin = connect("locklab-finalize")
        try:
            run.inventory_after = fetch_inventory(admin)
        finally:
            admin.close()

        run.wait_cycle = derive_wait_cycle(run)
        with run.lock:
            if run.status == "running":
                run.status = "cancelled" if run.error else "finished"
            run.finished_at = now_iso()
    except Exception as exc:  # noqa: BLE001
        log.exception("supervisor failed")
        run.error = f"编排异常: {exc}"
        run.status = "cancelled"
        run.finished_at = now_iso()
    finally:
        with _manager_lock:
            _active_run = None
        with run.lock:
            if run.finished_at is None:
                run.finished_at = now_iso()
        _runs[run.run_id] = run
        # 控制历史长度
        if len(_runs) > MAX_HISTORY:
            for old_id in sorted(
                _runs, key=lambda rid: _runs[rid].created_at
            )[: len(_runs) - MAX_HISTORY]:
                _runs.pop(old_id, None)
        log.info("run %s finalized: %s", run.run_id, run.status)


# --------------------------------------------------------------------------- #
# HTTP 接口
# --------------------------------------------------------------------------- #
@app.route("/")
def index():
    return render_template("index.html", transfers=TRANSFERS)


@app.route("/api/runs", methods=["POST"])
def start_run():
    global _active_run
    payload = request.get_json(silent=True) or {}
    lock_order = payload.get("lock_order", "opposite")
    if lock_order not in ("same", "opposite"):
        return jsonify({"error": "lock_order 只能是 same 或 opposite"}), 400

    try:
        lock_timeout_ms = max(100, min(600000, int(payload.get("lock_timeout_ms", 15000))))
        deadlock_timeout_ms = max(50, min(60000, int(payload.get("deadlock_timeout_ms", 1000))))
        stagger_ms = max(0, min(10000, int(payload.get("stagger_ms", 1200))))
        hard_timeout_s = max(5.0, min(300.0, float(payload.get("hard_timeout_s", 30.0))))
    except (TypeError, ValueError):
        return jsonify({"error": "参数格式不正确"}), 400

    with _manager_lock:
        if _active_run is not None and _active_run.status == "running":
            return jsonify({"error": "已有一轮实验在运行，请先取消或等待结束",
                            "run_id": _active_run.run_id}), 409
        run = Run(
            run_id=uuid.uuid4().hex[:12],
            lock_order=lock_order,
            lock_timeout_ms=lock_timeout_ms,
            deadlock_timeout_ms=deadlock_timeout_ms,
            stagger_ms=stagger_ms,
            hard_timeout_s=hard_timeout_s,
            workers={},
        )
        _active_run = run
        _runs[run.run_id] = run

    threading.Thread(target=run_supervisor, args=(run,), name="supervisor", daemon=True).start()
    return jsonify({"run_id": run.run_id}), 201


@app.route("/api/runs/<run_id>")
def get_run(run_id):
    run = _runs.get(run_id)
    if run is None:
        return jsonify({"error": "轮次不存在"}), 404
    return jsonify(run.public())


@app.route("/api/runs/<run_id>/cancel", methods=["POST"])
def cancel_run(run_id):
    run = _runs.get(run_id)
    if run is None:
        return jsonify({"error": "轮次不存在"}), 404
    run.cancel_event.set()
    run.error = (run.error or "") + "；用户手动取消，正在释放本轮连接"
    # 让被 lock_timeout 之外阻塞的语句也能尽快退出
    for key, conn in list(run.conns.items()):
        pid = run.workers.get(key, {}).get("pid")
        if pid:
            try:
                admin = connect("locklab-cancel")
                try:
                    with admin.cursor() as cur:
                        cur.execute("SELECT pg_cancel_backend(%s)", (pid,))
                    admin.commit()
                finally:
                    admin.close()
            except Exception as exc:  # noqa: BLE001
                log.warning("cancel failed for %s: %s", pid, exc)
    return jsonify({"ok": True})


@app.route("/api/runs")
def list_runs():
    items = [
        {
            "run_id": r.run_id,
            "lock_order": r.lock_order,
            "status": r.status,
            "created_at": r.created_at,
            "db_declared_deadlock": r.db_declared_deadlock,
            "deadlock_victim": r.deadlock_victim,
        }
        for r in sorted(_runs.values(), key=lambda x: x.created_at, reverse=True)
    ]
    return jsonify(items)


@app.route("/api/health")
def health():
    try:
        conn = connect("locklab-health")
        try:
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                cur.fetchone()
        finally:
            conn.close()
        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 503


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8000, threaded=True)
