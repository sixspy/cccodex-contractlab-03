import hashlib, secrets, sqlite3, threading
from datetime import datetime
from pathlib import Path
from fastapi import FastAPI, Header, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data"
DB_PATH = DATA / "app.db"
FRONTEND = ROOT / "frontend"
SESSIONS: dict[str, str] = {}

# 兼容双写排队：同进程用线程锁，跨进程靠 SQLite BEGIN IMMEDIATE 的写锁。
# 后到者只记兼容争用，不改变本次写入结果。
COMPAT_LOCK = threading.Lock()
COMPAT_PATHS = ("甲", "乙")

def begin_write(conn) -> bool:
    """取得本库写锁；返回是否因前一名值班持锁而排队（争用）。"""
    conn.execute("PRAGMA busy_timeout=0")
    try:
        conn.execute("BEGIN IMMEDIATE")
        conn.execute("PRAGMA busy_timeout=10000")
        return False
    except sqlite3.OperationalError as exc:
        if "locked" not in str(exc) and "busy" not in str(exc):
            raise
        conn.execute("PRAGMA busy_timeout=10000")
        conn.execute("BEGIN IMMEDIATE")
        return True

def connect():
    DATA.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, timeout=10)
    conn.row_factory = sqlite3.Row
    # 显式事务：多写操作自行 BEGIN IMMEDIATE / COMMIT，避免隐式事务吞掉整笔回滚。
    conn.isolation_level = None
    conn.execute("PRAGMA foreign_keys=ON")
    return conn

def sha(p: str) -> str:
    return hashlib.sha256(p.encode("utf-8")).hexdigest()

def now() -> str:
    return datetime.now().isoformat(timespec="seconds")

def today() -> str:
    return datetime.now().date().isoformat()

def ensure_users(conn):
    conn.execute("""CREATE TABLE IF NOT EXISTS users(
        username TEXT PRIMARY KEY, password_sha TEXT NOT NULL, role TEXT NOT NULL)""")
    if conn.execute("SELECT COUNT(*) c FROM users").fetchone()["c"] == 0:
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("admin", sha("123456"), "管理员"))
        conn.execute("INSERT INTO users VALUES (?,?,?)", ("viewer", sha("view123"), "只读"))

class LoginIn(BaseModel):
    username: str
    password: str

def require_user(authorization: str | None) -> str:
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(401, "未登录")
    u = SESSIONS.get(authorization.removeprefix("Bearer ").strip())
    if not u:
        raise HTTPException(401, "登录已失效")
    return u

PRODUCT = "入站契约"
SKELETON = "Schema 契约版本约束入站校验，破坏性变更进缓冲或拒收，并与落库结果对账"

app = FastAPI(title=PRODUCT)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])

POLICY_SEED = [
    ("兼容写入", "规则簿唯一路径", "兼容写入分甲、乙两支，只贯彻兼容规则簿选定的一支；入站列表与缓冲队列两侧不得混用兼容支。"),
    ("兼容写入", "甲路径：同事务整笔", "标缓冲与写缓冲行在同一事务内完成；任一步失败则整笔回滚，不留下批次或缓冲行。"),
    ("兼容写入", "乙路径：草稿后挂行", "先落缓冲草稿批次并确认，再原子挂缓冲行；草稿批次不得标为通过，挂行失败即回滚至草稿。"),
    ("兼容对账", "边界禁令", "禁止有缓冲结果而无缓冲行；禁止缓冲行指向不存在的批次。偏离规则簿一律拒绝并记兼容偏了。"),
    ("兼容并发", "抢点排队", "两名值班同时抢写兼容双写须排队，后到者只记兼容争用，不影响本次写入结果。"),
]

def init_db():
    with connect() as conn:
        ensure_users(conn)
        conn.executescript("""
        CREATE TABLE IF NOT EXISTS contracts(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          version TEXT UNIQUE NOT NULL,
          subject TEXT NOT NULL,
          mode TEXT NOT NULL,
          track TEXT NOT NULL,
          status TEXT NOT NULL,
          required_fields TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS ingress_batches(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          subject TEXT NOT NULL,
          payload_fields TEXT NOT NULL,
          contract_version TEXT,
          result TEXT NOT NULL,
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL,
          write_path TEXT,
          draft INTEGER NOT NULL DEFAULT 0
        );
        CREATE TABLE IF NOT EXISTS buffers(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL,
          reason TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS compat_rulebook(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          path TEXT NOT NULL,
          selected_on TEXT NOT NULL,
          selected_at TEXT NOT NULL,
          note TEXT NOT NULL DEFAULT ''
        );
        CREATE TABLE IF NOT EXISTS compat_events(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          kind TEXT NOT NULL,
          subject TEXT NOT NULL DEFAULT '',
          detail TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pending_reviews(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          batch_id INTEGER NOT NULL UNIQUE,
          subject TEXT NOT NULL,
          missing TEXT NOT NULL,
          status TEXT NOT NULL,
          created_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS policy_notes(
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          section TEXT NOT NULL,
          title TEXT NOT NULL,
          body TEXT NOT NULL,
          sort INTEGER NOT NULL DEFAULT 0
        );
        """)
        cols = {r["name"] for r in conn.execute("PRAGMA table_info(ingress_batches)").fetchall()}
        if "write_path" not in cols:
            conn.execute("ALTER TABLE ingress_batches ADD COLUMN write_path TEXT")
        if "draft" not in cols:
            conn.execute("ALTER TABLE ingress_batches ADD COLUMN draft INTEGER NOT NULL DEFAULT 0")
        # 不删历史数据：用触发器挡重复缓冲行，重复留给对账账册标为违规。
        conn.executescript("""
        CREATE TRIGGER IF NOT EXISTS trg_buffers_single
        BEFORE INSERT ON buffers
        FOR EACH ROW WHEN EXISTS (SELECT 1 FROM buffers WHERE batch_id=NEW.batch_id)
        BEGIN SELECT RAISE(ABORT, '同一批次不得重复挂缓冲行'); END;
        CREATE TRIGGER IF NOT EXISTS trg_buffers_batch_exists
        BEFORE INSERT ON buffers
        FOR EACH ROW WHEN NOT EXISTS (SELECT 1 FROM ingress_batches WHERE id=NEW.batch_id)
        BEGIN SELECT RAISE(ABORT, '缓冲行指向不存在批次'); END;
        CREATE TRIGGER IF NOT EXISTS trg_draft_insert_not_pass
        BEFORE INSERT ON ingress_batches
        FOR EACH ROW WHEN NEW.draft=1 AND NEW.result='通过'
        BEGIN SELECT RAISE(ABORT, '缓冲草稿不得标通过'); END;
        CREATE TRIGGER IF NOT EXISTS trg_draft_update_not_pass
        BEFORE UPDATE OF result ON ingress_batches
        FOR EACH ROW WHEN NEW.draft=1 AND NEW.result='通过'
        BEGIN SELECT RAISE(ABORT, '缓冲草稿不得标通过'); END;
        """)
        if conn.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"] == 0:
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v3","订单入站","严格","正式","启用","order_id,amount,currency"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v4-shadow","订单入站","兼容","影子","试跑","order_id,amount,currency,channel"))
            conn.execute("INSERT INTO contracts(version,subject,mode,track,status,required_fields) VALUES (?,?,?,?,?,?)",
                         ("v2","库存入站","严格","正式","停用","sku,qty"))
    # 现行库只铺了影子轨兼容契约；再铺一条正式轨兼容契约，兼容双写边界才可核。
    conn.execute("""INSERT INTO contracts(version,subject,mode,track,status,required_fields)
        SELECT 'v5','退货入站','兼容','正式','启用','return_id,order_id,reason,channel'
        WHERE NOT EXISTS (SELECT 1 FROM contracts WHERE subject='退货入站' AND track='正式' AND mode='兼容')""")
    if conn.execute("SELECT COUNT(*) c FROM policy_notes").fetchone()["c"] == 0:
        for i, (section, title, body) in enumerate(POLICY_SEED):
            conn.execute("INSERT INTO policy_notes(section,title,body,sort) VALUES (?,?,?,?)",
                         (section, title, body, i))

@app.on_event("startup")
def _s():
    init_db()

def record_event(conn, kind: str, detail: str, subject: str = ""):
    conn.execute("INSERT INTO compat_events(kind,subject,detail,created_at) VALUES (?,?,?,?)",
                 (kind, subject, detail, now()))

def current_rulebook(conn):
    row = conn.execute("SELECT * FROM compat_rulebook ORDER BY id DESC LIMIT 1").fetchone()
    return dict(row) if row else None

# ---- 兼容写入的内部步骤（拆开便于整笔回滚验收） ----
def insert_batch(conn, subject, payload, version, result, detail, write_path, draft):
    cur = conn.execute(
        "INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at,write_path,draft) VALUES (?,?,?,?,?,?,?,?)",
        (subject, payload, version, result, detail, now(), write_path, draft))
    return cur.lastrowid

def attach_buffer_row(conn, batch_id, reason):
    conn.execute("INSERT INTO buffers(batch_id,reason) VALUES (?,?)", (batch_id, reason))

def confirm_draft(conn, batch_id, detail):
    cur = conn.execute(
        "UPDATE ingress_batches SET result='缓冲', draft=0, detail=? WHERE id=? AND draft=1 AND result='草稿'",
        (detail, batch_id))
    if cur.rowcount != 1:
        raise RuntimeError("草稿批次确认失败")

def attach_pending(conn, batch_id, subject, missing):
    conn.execute("INSERT INTO pending_reviews(batch_id,subject,missing,status,created_at) VALUES (?,?,?,?,?)",
                 (batch_id, subject, ",".join(missing), "候审", now()))

def _rollback_quietly(conn):
    try:
        conn.execute("ROLLBACK")
    except sqlite3.Error:
        pass

def compat_buffer(subject: str, payload: str, version: str, missing: list[str], requested_path: str | None):
    """兼容双写边界：兼容提交一律批次+缓冲行同行，同成同败；规则簿当天不得更换。"""
    contended = not COMPAT_LOCK.acquire(blocking=False)
    if contended:
        COMPAT_LOCK.acquire()  # 排队等前一名值班写完
    conn = connect()
    try:
        try:
            contended = begin_write(conn) or contended
            rule = current_rulebook(conn)
            if rule is None:
                if requested_path and requested_path not in COMPAT_PATHS:
                    _rollback_quietly(conn)
                    record_event(conn, "兼容偏了", f"非法兼容路径:{requested_path}", subject)
                    raise HTTPException(400, "兼容路径仅甲/乙，已记兼容偏了")
                chosen = requested_path or "甲"
                conn.execute("INSERT INTO compat_rulebook(path,selected_on,selected_at,note) VALUES (?,?,?,?)",
                             (chosen, today(), now(), "首次兼容写入选定" + chosen + "路径"))
                rule = current_rulebook(conn)
            elif requested_path and requested_path != rule["path"]:
                _rollback_quietly(conn)
                record_event(conn, "兼容偏了", f"提交路径{requested_path}偏离规则簿{rule['path']}", subject)
                raise HTTPException(409, "兼容写入路径偏离兼容规则簿，已记兼容偏了")
            if contended:
                record_event(conn, "兼容争用", f"兼容双写排队后到，按规则簿{rule['path']}路径写入", subject)
            detail = ("兼容双写;缺字段:" + ",".join(missing)) if missing else "兼容双写;字段齐全仍入缓冲"
            rule_path = rule["path"]
            if rule_path == "甲":
                # 甲：标缓冲、缓冲行、候审卡片与规则簿校验同一事务，失败整笔回滚。
                try:
                    batch_id = insert_batch(conn, subject, payload, version, "缓冲", detail, "甲", 0)
                    attach_buffer_row(conn, batch_id, "兼容双写缓冲（甲：标记与缓冲行同一事务）")
                    attach_pending(conn, batch_id, subject, missing)
                    conn.execute("COMMIT")
                except Exception:
                    _rollback_quietly(conn)
                    record_event(conn, "兼容偏了", "甲路径整笔回滚：批次与缓冲行均未落库", subject)
                    raise HTTPException(500, "兼容双写整笔回滚")
            else:
                # 乙：先落缓冲草稿批次（草稿不得标通过），确认后再原子挂行。
                try:
                    batch_id = insert_batch(conn, subject, payload, version, "草稿", "草稿待确认:" + detail, "乙", 1)
                    conn.execute("COMMIT")
                except Exception:
                    _rollback_quietly(conn)
                    record_event(conn, "兼容偏了", "乙路径草稿批次落库失败，未产生任何批次", subject)
                    raise HTTPException(500, "兼容双写草稿落库失败")
                try:
                    begin_write(conn)
                    attach_buffer_row(conn, batch_id, "兼容双写缓冲（乙：草稿确认后挂行）")
                    confirm_draft(conn, batch_id, detail)
                    attach_pending(conn, batch_id, subject, missing)
                    conn.execute("COMMIT")
                except Exception:
                    _rollback_quietly(conn)
                    record_event(conn, "兼容偏了", "乙路径挂行失败，已回滚缓冲行并保留未标通过的草稿", subject)
                    raise HTTPException(500, "兼容双写挂行失败，草稿未标通过")
        finally:
            if conn.in_transaction:
                _rollback_quietly(conn)
        pending = conn.execute("SELECT id FROM pending_reviews WHERE batch_id=?", (batch_id,)).fetchone()
        return {"result": "缓冲", "version": version, "missing": missing, "batch_id": batch_id,
                "write_path": rule_path, "pending_id": pending["id"] if pending else None}
    finally:
        conn.close()
        COMPAT_LOCK.release()

@app.get("/api/health")
def health():
    with connect() as c:
        n = c.execute("SELECT COUNT(*) c FROM contracts").fetchone()["c"]
    return {"ok": True, "product": PRODUCT, "skeleton": SKELETON, "contracts": n, "db": str(DB_PATH)}

@app.post("/api/login")
def login(body: LoginIn):
    with connect() as c:
        row = c.execute("SELECT username,role FROM users WHERE username=? AND password_sha=?", (body.username, sha(body.password))).fetchone()
    if not row: raise HTTPException(401, "账号或密码错误")
    tok = secrets.token_hex(16); SESSIONS[tok]=row["username"]
    return {"token": tok, "username": row["username"], "role": row["role"]}

@app.get("/api/contracts")
def contracts(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM contracts ORDER BY id").fetchall()]}

def pick_contract(conn, subject: str):
    # 只在正式轨启用契约里取；不把影子/试跑轨混进正式入站，避免两侧混用兼容支。
    return conn.execute("SELECT * FROM contracts WHERE subject=? AND track='正式' AND status='启用' ORDER BY id DESC LIMIT 1", (subject,)).fetchone()

class IngressIn(BaseModel):
    subject: str
    payload_fields: str  # comma fields present
    write_path: str | None = None  # 可选：须与兼容规则簿一致

@app.post("/api/ingress/validate")
def validate(body: IngressIn, authorization: str | None = Header(default=None)):
    require_user(authorization)
    fields = {x.strip() for x in body.payload_fields.split(",") if x.strip()}
    with connect() as c:
        ctr = pick_contract(c, body.subject)
    if not ctr:
        raise HTTPException(404, "无启用正式契约")
    need = {x.strip() for x in ctr["required_fields"].split(",") if x.strip()}
    missing = sorted(need - fields)
    if ctr["mode"] == "兼容":
        # 兼容模式一律双写缓冲，批次与缓冲行同行；不因字段齐全而放行成“通过”。
        return compat_buffer(body.subject, body.payload_fields, ctr["version"], missing, body.write_path)
    if missing:
        with connect() as c:
            cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                            (body.subject, body.payload_fields, ctr["version"], "拒收", "缺字段:"+ ",".join(missing), now()))
            return {"result":"拒收", "version": ctr["version"], "missing": missing, "batch_id": cur.lastrowid}
    with connect() as c:
        cur = c.execute("INSERT INTO ingress_batches(subject,payload_fields,contract_version,result,detail,created_at) VALUES (?,?,?,?,?,?)",
                        (body.subject, body.payload_fields, ctr["version"], "通过", "字段齐全", now()))
        return {"result":"通过", "version": ctr["version"], "batch_id": cur.lastrowid}

@app.get("/api/ingress")
def ingress(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return {"items":[dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC").fetchall()]}

@app.get("/api/buffers")
def buffers(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("""SELECT b.*, i.subject, i.payload_fields, i.result AS batch_result, i.write_path, i.draft
                            FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/pending")
def pending(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM pending_reviews ORDER BY id DESC").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/policy")
def policy(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM policy_notes ORDER BY sort, id").fetchall()
        return {"items":[dict(r) for r in rows]}

@app.get("/api/compat/events")
def compat_events(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        rows = c.execute("SELECT * FROM compat_events ORDER BY id DESC").fetchall()
        return {"items":[dict(r) for r in rows]}

class RulebookIn(BaseModel):
    path: str
    operator: str | None = None

@app.get("/api/compat/rulebook")
def get_rulebook(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        cur = current_rulebook(c)
        history = [dict(r) for r in c.execute("SELECT * FROM compat_rulebook ORDER BY id DESC").fetchall()]
    return {"current": cur, "history": history}

@app.post("/api/compat/rulebook")
def set_rulebook(body: RulebookIn, authorization: str | None = Header(default=None)):
    user = require_user(authorization)
    if body.path not in COMPAT_PATHS:
        with connect() as c:
            record_event(c, "兼容偏了", f"非法兼容路径:{body.path}")
        raise HTTPException(400, "兼容路径仅甲/乙，已记兼容偏了")
    with COMPAT_LOCK:
        conn = connect()
        try:
            begin_write(conn)
            cur = current_rulebook(conn)
            stamp = today()
            if cur and cur["selected_on"] == stamp and cur["path"] != body.path:
                _rollback_quietly(conn)
                record_event(conn, "兼容偏了", f"当天试图把规则簿由{cur['path']}改为{body.path}，拒绝更换")
                raise HTTPException(409, "兼容规则簿当天不得更换，已记兼容偏了")
            if cur and cur["path"] == body.path:
                _rollback_quietly(conn)
                return {"current": cur, "changed": False}
            conn.execute("INSERT INTO compat_rulebook(path,selected_on,selected_at,note) VALUES (?,?,?,?)",
                         (body.path, stamp, now(), (body.operator or user)))
            conn.execute("COMMIT")
            cur = current_rulebook(conn)
            return {"current": cur, "changed": True}
        finally:
            if conn.in_transaction:
                _rollback_quietly(conn)
            conn.close()

def reconcile_data(c):
    rule = current_rulebook(c)
    rule_path = rule["path"] if rule else None
    total = c.execute("SELECT COUNT(*) c FROM ingress_batches").fetchone()["c"]
    passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='通过'").fetchone()["c"]
    buffered = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='缓冲'").fetchone()["c"]
    rejected = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE result='拒收'").fetchone()["c"]
    drafts = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE draft=1").fetchone()["c"]
    buf_rows = c.execute("SELECT COUNT(*) c FROM buffers").fetchone()["c"]
    # 集合级边界：不只看两个计数是否相等。
    unbacked = c.execute("""SELECT COUNT(*) c FROM ingress_batches i
        LEFT JOIN buffers b ON b.batch_id=i.id WHERE i.result='缓冲' AND b.id IS NULL""").fetchone()["c"]
    orphan = c.execute("""SELECT COUNT(*) c FROM buffers b
        LEFT JOIN ingress_batches i ON i.id=b.batch_id WHERE i.id IS NULL""").fetchone()["c"]
    draft_passed = c.execute("SELECT COUNT(*) c FROM ingress_batches WHERE draft=1 AND result='通过'").fetchone()["c"]
    duplicate_buffer = c.execute("""SELECT COALESCE(SUM(n-1), 0) c FROM
        (SELECT COUNT(*) n FROM buffers GROUP BY batch_id HAVING n>1)""").fetchone()["c"]
    # 两侧不得混兼容支：每个缓冲批次（及其缓冲行）都必须落在其写入当日生效的规则簿那一支。
    rule_in_force = """COALESCE((SELECT r.path FROM compat_rulebook r
        WHERE r.selected_on <= date(i.created_at) ORDER BY r.selected_on DESC, r.id DESC LIMIT 1), i.write_path)"""
    mixed = c.execute(f"""SELECT COUNT(*) c FROM ingress_batches i
        WHERE i.result='缓冲' AND i.write_path IS NOT NULL AND i.write_path <> {rule_in_force}""").fetchone()["c"]
    mixed += c.execute(f"""SELECT COUNT(*) c FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id
        WHERE i.write_path IS NOT NULL AND i.write_path <> {rule_in_force}""").fetchone()["c"]
    pending_open = c.execute("SELECT COUNT(*) c FROM pending_reviews WHERE status='候审'").fetchone()["c"]
    pending_missing = c.execute("""SELECT COUNT(*) c FROM ingress_batches i
        LEFT JOIN pending_reviews p ON p.batch_id=i.id
        WHERE i.result='缓冲' AND i.write_path IS NOT NULL AND p.id IS NULL""").fetchone()["c"]
    # 草稿未确认即兼容写入未完成；草稿本身不是“缓冲结果”，故单列而不算无行缓冲。
    violations = unbacked + orphan + duplicate_buffer + drafts + draft_passed + mixed + pending_missing
    return {
        "ingress_total": total, "passed": passed, "buffered": buffered, "rejected": rejected,
        "draft_open": drafts, "buffer_rows": buf_rows,
        "unbacked_buffered": unbacked, "orphan_buffer_rows": orphan, "duplicate_buffer_rows": duplicate_buffer,
        "draft_passed": draft_passed,
        "mixed_branch": mixed, "pending_open": pending_open, "pending_missing": pending_missing,
        "rulebook_path": rule_path, "rulebook_day": rule["selected_on"] if rule else None,
        "violations": violations,
        "aligned": violations == 0 and buffered == buf_rows,
    }

@app.get("/api/reconcile")
def reconcile(authorization: str | None = Header(default=None)):
    require_user(authorization)
    with connect() as c:
        return reconcile_data(c)

@app.get("/api/compat/board")
def compat_board(authorization: str | None = Header(default=None)):
    """兼容页序丙：入站列表、缓冲队列、对账账册、策略附页、候审卡片同屏可核。"""
    require_user(authorization)
    with connect() as c:
        return {
            "ingress": [dict(r) for r in c.execute("SELECT * FROM ingress_batches ORDER BY id DESC LIMIT 50").fetchall()],
            "buffers": [dict(r) for r in c.execute("""SELECT b.*, i.subject, i.payload_fields, i.result AS batch_result, i.write_path, i.draft
                FROM buffers b JOIN ingress_batches i ON i.id=b.batch_id ORDER BY b.id DESC""").fetchall()],
            "reconcile": reconcile_data(c),
            "policy": [dict(r) for r in c.execute("SELECT * FROM policy_notes ORDER BY sort, id").fetchall()],
            "pending": [dict(r) for r in c.execute("SELECT * FROM pending_reviews ORDER BY id DESC").fetchall()],
            "events": [dict(r) for r in c.execute("SELECT * FROM compat_events ORDER BY id DESC LIMIT 20").fetchall()],
            "rulebook": current_rulebook(c),
        }

@app.get("/")
def index():
    return FileResponse(FRONTEND / "index.html")
app.mount("/static", StaticFiles(directory=str(FRONTEND)), name="static")
