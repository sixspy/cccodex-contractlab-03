import itertools
import sqlite3
import threading

import pytest
from fastapi.testclient import TestClient
from app import app, init_db, connect

COMPAT_SUBJECT = "订单入站兼容"


@pytest.fixture(autouse=True)
def _db(tmp_path, monkeypatch):
    monkeypatch.setattr("app.DB_PATH", tmp_path / "t.db")
    init_db()
    # 单独铺一个“正式轨 + 兼容模式”的启用契约，兼容双写才有阵地。
    with connect() as c:
        c.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                  ("v5-compat", COMPAT_SUBJECT, "兼容", "正式", "启用", "order_id,amount,currency,channel"))
    yield


def client():
    c = TestClient(app)
    tok = c.post("/api/login", json={"username": "admin", "password": "123456"}).json()["token"]
    return c, {"Authorization": f"Bearer {tok}"}


def submit(c, h, fields="order_id,amount", path=None):
    body = {"subject": COMPAT_SUBJECT, "payload_fields": fields}
    if path:
        body["write_path"] = path
    return c.post("/api/ingress/validate", headers=h, json=body)


def test_strict_paths():
    c, h = client()
    assert c.get("/api/health").json()["product"] == "入站契约"
    r = c.post("/api/ingress/validate", headers=h, json={"subject": "订单入站", "payload_fields": "order_id,amount"})
    assert r.status_code == 200 and r.json()["result"] == "拒收"
    ok = c.post("/api/ingress/validate", headers=h, json={"subject": "订单入站", "payload_fields": "order_id,amount,currency"})
    assert ok.json()["result"] == "通过"
    assert c.get("/api/reconcile", headers=h).json()["passed"] >= 1


def test_compat_batch_and_buffer_row_land_together_path_jia():
    c, h = client()
    r = submit(c, h)
    body = r.json()
    assert body["result"] == "缓冲" and body["write_path"] == "甲"
    bufs = c.get("/api/buffers", headers=h).json()["items"]
    assert len(bufs) == 1 and bufs[0]["batch_id"] == body["batch_id"]
    assert bufs[0]["batch_result"] == "缓冲" and bufs[0]["draft"] == 0
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["buffered"] == recon["buffer_rows"] == 1 and recon["aligned"] is True
    assert recon["violations"] == 0


def test_compat_full_fields_still_dual_writes_not_passed():
    c, h = client()
    body = submit(c, h, fields="order_id,amount,currency,channel").json()
    assert body["result"] == "缓冲"
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["passed"] == 0 and recon["buffered"] == 1 and recon["buffer_rows"] == 1


def test_path_jia_failure_rolls_back_whole_batch(monkeypatch):
    import app as mod

    c, h = client()

    def boom(conn, batch_id, reason):
        raise RuntimeError("挂缓冲行失败")

    monkeypatch.setattr(mod, "attach_buffer_row", boom)
    r = submit(c, h)
    assert r.status_code == 500
    recon = c.get("/api/reconcile", headers=h).json()
    # 整笔回滚：既没有“缓冲结果无行”，也没有残留批次。
    assert recon["ingress_total"] == 0 and recon["buffered"] == 0 and recon["buffer_rows"] == 0
    assert recon["unbacked_buffered"] == 0 and recon["violations"] == 0
    events = c.get("/api/compat/events", headers=h).json()["items"]
    assert any(e["kind"] == "兼容偏了" for e in events)


def test_path_yi_draft_then_attach_and_never_passed(monkeypatch):
    import app as mod

    c, h = client()
    assert c.post("/api/compat/rulebook", headers=h, json={"path": "乙"}).json()["changed"] is True

    # 正常乙路径：草稿确认后挂行，最终缓冲且草稿标记清除。
    body = submit(c, h).json()
    assert body["result"] == "缓冲" and body["write_path"] == "乙"
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["buffered"] == 1 and recon["buffer_rows"] == 1 and recon["draft_open"] == 0
    assert recon["draft_passed"] == 0 and recon["aligned"] is True

    # 挂行失败：草稿保留、不得标通过、不得有缓冲行。
    def boom(*a, **k):
        raise RuntimeError("挂行失败")

    monkeypatch.setattr(mod, "attach_buffer_row", boom)
    r = submit(c, h)
    assert r.status_code == 500
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["buffer_rows"] == 1  # 仍是上一条成功写入的行
    assert recon["draft_open"] == 1 and recon["draft_passed"] == 0
    # 草稿不是“缓冲结果”，因此不是无行缓冲；但未确认草稿本身即违规。
    assert recon["unbacked_buffered"] == 0 and recon["orphan_buffer_rows"] == 0
    assert recon["violations"] >= 1
    assert recon["aligned"] is False
    with connect() as conn:
        row = conn.execute("SELECT result, draft FROM ingress_batches ORDER BY id DESC LIMIT 1").fetchone()
    assert row["draft"] == 1 and row["result"] == "草稿"


def test_draft_cannot_mark_passed_at_db_level():
    with connect() as conn:
        cur = conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,draft) "
            "VALUES ('订单入站','x','v5','草稿','草稿',datetime('now'),1)")
        bid = cur.lastrowid
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("UPDATE ingress_batches SET result='通过' WHERE id=?", (bid,))
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute(
                "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,write_path,draft) "
                "VALUES ('订单入站','x','v5','通过','x',datetime('now'),'乙',1)")


def test_buffer_row_cannot_point_at_missing_batch():
    with connect() as conn:
        with pytest.raises(sqlite3.IntegrityError):
            conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (99999,'悬空')")


def test_rulebook_locked_for_the_day():
    c, h = client()
    assert c.post("/api/compat/rulebook", headers=h, json={"path": "甲"}).json()["changed"] is True
    r = c.post("/api/compat/rulebook", headers=h, json={"path": "乙"})
    assert r.status_code == 409
    assert c.get("/api/compat/rulebook", headers=h).json()["current"]["path"] == "甲"
    events = c.get("/api/compat/events", headers=h).json()["items"]
    assert any(e["kind"] == "兼容偏了" for e in events)


def test_submit_with_off_rulebook_path_refused_and_logged():
    c, h = client()
    c.post("/api/compat/rulebook", headers=h, json={"path": "甲"})
    r = submit(c, h, path="乙")
    assert r.status_code == 409
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["ingress_total"] == 0 and recon["buffer_rows"] == 0
    events = c.get("/api/compat/events", headers=h).json()["items"]
    assert any(e["kind"] == "兼容偏了" and "乙" in e["detail"] for e in events)


def test_mixed_branch_detected_by_ledger():
    c, h = client()
    submit(c, h)  # 甲写入
    with connect() as conn:
        # 绕过接口伪造一条乙支缓冲批次+行，模拟“两侧混兼容支”。
        cur = conn.execute(
            "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,write_path,draft) "
            "VALUES (?,?,?,?,?,datetime('now'),'乙',0)", (COMPAT_SUBJECT, "order_id", "v5", "缓冲", "伪造"))
        conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?, '伪造')", (cur.lastrowid,))
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["mixed_branch"] >= 2  # 批次侧 + 缓冲行侧各记一次
    assert recon["aligned"] is False


def test_concurrent_dual_write_queues_and_logs_contention():
    import app as mod

    c, h = client()
    c.post("/api/compat/rulebook", headers=h, json={"path": "甲"})

    hold = threading.Event()
    release = threading.Event()
    orig_insert_batch = mod.insert_batch
    calls = itertools.count()

    def slow_insert_batch(conn, *a, **k):
        if next(calls) == 0:
            hold.set()
            release.wait(10)  # 第一名值班持闸不放，确保第二名必然撞闸排队
        return orig_insert_batch(conn, *a, **k)

    mod.insert_batch = slow_insert_batch
    results = []
    try:
        def worker():
            cc, hh = client()
            results.append(submit(cc, hh).json())

        first = threading.Thread(target=worker, name="first")
        second = threading.Thread(target=worker, name="second")
        first.start()
        assert hold.wait(10)
        second.start()
        threading.Event().wait(0.5)  # 确保第二名已到闸口排队
        release.set()
        first.join(10)
        second.join(10)
    finally:
        mod.insert_batch = orig_insert_batch
        release.set()
    assert len(results) == 2 and all(r["result"] == "缓冲" for r in results)
    recon = c.get("/api/reconcile", headers=h).json()
    assert recon["buffered"] == 2 and recon["buffer_rows"] == 2 and recon["aligned"] is True
    events = c.get("/api/compat/events", headers=h).json()["items"]
    assert sum(1 for e in events if e["kind"] == "兼容争用") == 1


def test_compat_board_exposes_all_five_panels():
    c, h = client()
    submit(c, h)
    board = c.get("/api/compat/board", headers=h).json()
    for key in ("ingress", "buffers", "reconcile", "policy", "pending"):
        assert key in board
    assert len(board["ingress"]) == 1 and len(board["buffers"]) == 1
    assert len(board["pending"]) == 1 and len(board["policy"]) >= 3
    assert board["reconcile"]["aligned"] is True
