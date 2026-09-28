"""SQLite 持久层：不可变特征库版本、判定记录与现场设备批次链。

特征库以版本为单位不可变存储：

- ``library_versions``：版本元数据，状态机 draft → active → historical；
- ``library_version_aids``：每个版本的完整航标快照（版本不可变，可复算差异）；
- ``library_state``：单行表，指向当前激活版本。

现场设备批次链：

- ``device_batches``：每个设备按 sequence 递增的批次记录（含观测时间戳、
  /judge 四项阈值、批次摘要与首次受理响应），(device_id, sequence) 与
  (device_id, batch_id) 均唯一。``status`` 固定为首次受理事实 ``accepted``，
  另以 ``replay_count`` / ``last_replayed_at`` 持久化幂等重放；
- ``device_chains``：每个设备的链头游标（last_sequence、head_digest）与
  最近一次事件（受理或重放）的状态/序号/时刻。

批次提交在单个事务内完成「幂等/链头校验 → 追加记录 → 更新链头」；重放也在
同一幂等事务内原子累加 ``replay_count`` 并刷新批次与链游标的最近事件——
不新增批次、不推进链头、不改写首次受理响应。写锁 + BEGIN IMMEDIATE +
唯一约束保证并发提交串行化，不会形成分叉、不会丢失重放计数。

首次启动时把旧版平铺 ``aids`` 表整体迁移为初始激活版本（source='migration'），
并给 ``judgments`` 表补充 ``library_version`` 列；同时为旧库的批次表补充
``replay_count`` / ``last_replayed_at`` 列、为链游标表补充最近事件三列
（旧记录重放次数为零，最近事件回填为链头批次的受理事件）。迁移在单个事务内
完成。
"""
from __future__ import annotations

import json
import sqlite3
import threading
import time
from contextlib import closing, contextmanager
from datetime import datetime, timezone
from pathlib import Path

from .batches import canonical_payload, compute_digest

SCHEMA = """
CREATE TABLE IF NOT EXISTS library_versions (
    version_id   INTEGER PRIMARY KEY AUTOINCREMENT,
    status       TEXT NOT NULL CHECK (status IN ('draft', 'active', 'historical')),
    base_version INTEGER REFERENCES library_versions(version_id),
    source       TEXT NOT NULL DEFAULT 'api',
    created_at   REAL NOT NULL,
    activated_at REAL
);
CREATE TABLE IF NOT EXISTS library_version_aids (
    version_id INTEGER NOT NULL REFERENCES library_versions(version_id),
    aid_id     TEXT NOT NULL,
    period     REAL NOT NULL,
    flashes    TEXT NOT NULL,
    PRIMARY KEY (version_id, aid_id)
);
CREATE TABLE IF NOT EXISTS library_state (
    id         INTEGER PRIMARY KEY CHECK (id = 1),
    version_id INTEGER NOT NULL REFERENCES library_versions(version_id)
);
CREATE TABLE IF NOT EXISTS judgments (
    id              INTEGER PRIMARY KEY AUTOINCREMENT,
    created_at      REAL NOT NULL,
    status          TEXT NOT NULL,
    best_aid_id     TEXT,
    matched         INTEGER NOT NULL DEFAULT 0,
    missed          INTEGER NOT NULL DEFAULT 0,
    false_count     INTEGER NOT NULL DEFAULT 0,
    drift           REAL,
    library_version INTEGER,
    request         TEXT NOT NULL,
    response        TEXT NOT NULL
);
CREATE TABLE IF NOT EXISTS device_chains (
    device_id           TEXT PRIMARY KEY,
    last_sequence       INTEGER NOT NULL,
    head_digest         TEXT NOT NULL,
    batch_count         INTEGER NOT NULL,
    updated_at          REAL NOT NULL,
    last_event_status   TEXT NOT NULL DEFAULT 'accepted'
        CHECK (last_event_status IN ('accepted', 'replayed')),
    last_event_sequence INTEGER NOT NULL DEFAULT 0,
    last_event_at       REAL NOT NULL DEFAULT 0.0
);
CREATE TABLE IF NOT EXISTS device_batches (
    id                INTEGER PRIMARY KEY AUTOINCREMENT,
    device_id         TEXT NOT NULL,
    sequence          INTEGER NOT NULL,
    batch_id          TEXT NOT NULL,
    previous_digest   TEXT NOT NULL,
    digest            TEXT NOT NULL,
    observation_count INTEGER NOT NULL,
    thresholds        TEXT NOT NULL,
    observations      TEXT NOT NULL,
    response          TEXT NOT NULL,
    status            TEXT NOT NULL DEFAULT 'accepted',
    replay_count      INTEGER NOT NULL DEFAULT 0,
    last_replayed_at  REAL,
    created_at        REAL NOT NULL,
    UNIQUE (device_id, sequence),
    UNIQUE (device_id, batch_id)
);
"""


class VersionNotFound(Exception):
    """引用的版本不存在。"""


class VersionStateError(Exception):
    """版本状态不允许该操作（如激活非草稿、以草稿为基线）。"""


class VersionConflict(Exception):
    """激活时草稿的 base_version 已不再是当前激活版本（乐观并发冲突）。"""


class BatchConflict(Exception):
    """批次上传冲突（HTTP 409）：载荷变化或链头过期，detail 附期望值。"""

    def __init__(
        self,
        *,
        reason: str,
        message: str,
        expected_sequence: int,
        expected_previous_digest: str,
        existing: dict | None = None,
    ):
        super().__init__(message)
        self.detail = {
            "error": "batch_conflict",
            "reason": reason,
            "message": message,
            "expected_sequence": expected_sequence,
            "expected_previous_digest": expected_previous_digest,
        }
        if existing is not None:
            self.detail["existing_batch"] = existing


def _iso(ts: float | None) -> str | None:
    if ts is None:
        return None
    return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()


def _diff_aids(base_aids: list[dict], new_aids: list[dict]) -> dict:
    """按航标编号比较两个快照，给出增/删/改（period 或 flashes 不同即为改）。"""
    base = {a["aid_id"]: a for a in base_aids}
    new = {a["aid_id"]: a for a in new_aids}
    return {
        "added": sorted(k for k in new if k not in base),
        "removed": sorted(k for k in base if k not in new),
        "modified": sorted(
            k for k in new.keys() & base.keys()
            if base[k]["period"] != new[k]["period"] or base[k]["flashes"] != new[k]["flashes"]
        ),
    }


class Database:
    """按操作开启连接的轻量封装：串行写由锁保证，多步写包在单个事务内。"""

    def __init__(self, path: str):
        self._path = path
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        with closing(self._connect()) as conn:
            conn.executescript(SCHEMA)
            try:
                conn.execute("BEGIN IMMEDIATE")
                self._migrate(conn)
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self._path)
        conn.row_factory = sqlite3.Row
        return conn

    @contextmanager
    def _write_tx(self):
        """串行化的单事务写：BEGIN IMMEDIATE … COMMIT，异常时 ROLLBACK。"""
        with self._lock, closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN IMMEDIATE")
                yield conn
                conn.commit()
            except Exception:
                conn.rollback()
                raise

    # ---------------- 首次迁移 ----------------

    def _migrate(self, conn) -> None:
        tables = {
            r[0] for r in conn.execute("SELECT name FROM sqlite_master WHERE type='table'")
        }
        if "aids" in tables:  # 旧版平铺特征库 → 初始激活版本
            rows = conn.execute(
                "SELECT aid_id, period, flashes FROM aids ORDER BY aid_id"
            ).fetchall()
            now = time.time()
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at, activated_at) "
                "VALUES ('active', NULL, 'migration', ?, ?)",
                (now, now),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [(vid, r["aid_id"], r["period"], r["flashes"]) for r in rows],
            )
            conn.execute("INSERT INTO library_state (id, version_id) VALUES (1, ?)", (vid,))
            conn.execute("DROP TABLE aids")
        cols = {r[1] for r in conn.execute("PRAGMA table_info(judgments)")}
        if "library_version" not in cols:
            conn.execute("ALTER TABLE judgments ADD COLUMN library_version INTEGER")
        self._migrate_device_tables(conn)

    def _migrate_device_tables(self, conn) -> None:
        """旧库兼容：批次表补 replay_count/last_replayed_at，链表补最近事件三列。

        旧记录的重放次数为零（``last_replayed_at`` 为 NULL）；链游标最近事件
        回填为当前链头批次的受理事件，与新批次受理后的游标含义保持一致。
        """
        batch_cols = {
            r[1] for r in conn.execute("PRAGMA table_info(device_batches)")
        }
        if "replay_count" not in batch_cols:
            conn.execute(
                "ALTER TABLE device_batches ADD COLUMN "
                "replay_count INTEGER NOT NULL DEFAULT 0"
            )
        if "last_replayed_at" not in batch_cols:
            conn.execute("ALTER TABLE device_batches ADD COLUMN last_replayed_at REAL")

        chain_cols = {
            r[1] for r in conn.execute("PRAGMA table_info(device_chains)")
        }
        migrated_chain = False
        if "last_event_status" not in chain_cols:
            conn.execute(
                "ALTER TABLE device_chains ADD COLUMN "
                "last_event_status TEXT NOT NULL DEFAULT 'accepted'"
            )
            migrated_chain = True
        if "last_event_sequence" not in chain_cols:
            conn.execute(
                "ALTER TABLE device_chains ADD COLUMN "
                "last_event_sequence INTEGER NOT NULL DEFAULT 0"
            )
            migrated_chain = True
        if "last_event_at" not in chain_cols:
            conn.execute(
                "ALTER TABLE device_chains ADD COLUMN "
                "last_event_at REAL NOT NULL DEFAULT 0.0"
            )
            migrated_chain = True
        if migrated_chain:
            # 旧链的最近事件即链头批次的首次受理：状态 accepted、序号为链头序号、
            # 时刻取该批次的 created_at
            conn.execute(
                "UPDATE device_chains SET "
                "last_event_status = 'accepted', "
                "last_event_sequence = last_sequence, "
                "last_event_at = COALESCE("
                "  (SELECT b.created_at FROM device_batches b "
                "   WHERE b.device_id = device_chains.device_id "
                "   AND b.sequence = device_chains.last_sequence), 0.0)"
            )

    # ---------------- 特征库版本 ----------------

    @staticmethod
    def _aids_of(conn, version_id: int) -> list[dict]:
        rows = conn.execute(
            "SELECT aid_id, period, flashes FROM library_version_aids "
            "WHERE version_id = ? ORDER BY aid_id",
            (version_id,),
        ).fetchall()
        return [
            {"aid_id": r["aid_id"], "period": r["period"], "flashes": json.loads(r["flashes"])}
            for r in rows
        ]

    def _version_meta(self, conn, row) -> dict:
        vid = row["version_id"]
        base = row["base_version"]
        count = conn.execute(
            "SELECT COUNT(*) AS c FROM library_version_aids WHERE version_id = ?", (vid,)
        ).fetchone()["c"]
        diff = None
        if base is not None:
            diff = _diff_aids(self._aids_of(conn, base), self._aids_of(conn, vid))
        return {
            "version_id": vid,
            "status": row["status"],
            "base_version": base,
            "source": row["source"],
            "aid_count": count,
            "created_at": _iso(row["created_at"]),
            "activated_at": _iso(row["activated_at"]) if row["activated_at"] is not None else None,
            "diff": diff,
        }

    def create_version(self, aids, *, base_version: int, source: str = "api") -> dict:
        """保存完整特征库为草稿版本；基线可以是当前激活版本，也可以是历史版本。"""
        rows = [(a.aid_id, a.period, json.dumps(a.flashes)) for a in aids]
        with self._write_tx() as conn:
            base = conn.execute(
                "SELECT status FROM library_versions WHERE version_id = ?", (base_version,)
            ).fetchone()
            if base is None:
                raise VersionNotFound(f"base_version {base_version} not found")
            if base["status"] == "draft":
                raise VersionStateError(
                    f"base_version {base_version} is still a draft; "
                    "base must be an activated (current or historical) version"
                )
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at) "
                "VALUES ('draft', ?, ?, ?)",
                (base_version, source, time.time()),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [(vid, aid_id, period, flashes) for aid_id, period, flashes in rows],
            )
        return self.get_version(vid)

    def activate_version(self, version_id: int) -> dict:
        """单事务激活：确认草稿的 base_version 仍是当前激活版本，然后原子切换。"""
        with self._write_tx() as conn:
            row = conn.execute(
                "SELECT * FROM library_versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            if row is None:
                raise VersionNotFound(f"version {version_id} not found")
            if row["status"] != "draft":
                raise VersionStateError(
                    f"version {version_id} is {row['status']}; only a draft can be activated"
                )
            state = conn.execute("SELECT version_id FROM library_state WHERE id = 1").fetchone()
            current = state["version_id"] if state is not None else None
            if row["base_version"] != current:
                raise VersionConflict(
                    f"base_version {row['base_version']} is no longer the active version "
                    f"(currently active: {current}); re-create the draft on top of the "
                    "current active version and retry"
                )
            now = time.time()
            conn.execute(
                "UPDATE library_versions SET status = 'historical' WHERE status = 'active'"
            )
            conn.execute(
                "UPDATE library_versions SET status = 'active', activated_at = ? "
                "WHERE version_id = ?",
                (now, version_id),
            )
            conn.execute(
                "INSERT INTO library_state (id, version_id) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET version_id = excluded.version_id",
                (version_id,),
            )
        return self.get_version(version_id)

    def publish_import(self, aids, *, replace: bool) -> tuple[int, dict]:
        """/library/import：基于当前激活版本合并/替换，单事务创建并直接激活新版本。"""
        with self._write_tx() as conn:
            state = conn.execute("SELECT version_id FROM library_state WHERE id = 1").fetchone()
            base_id = state["version_id"] if state is not None else None
            merged: dict[str, dict] = {}
            if not replace and base_id is not None:
                for a in self._aids_of(conn, base_id):
                    merged[a["aid_id"]] = a
            for a in aids:
                merged[a.aid_id] = {
                    "aid_id": a.aid_id,
                    "period": a.period,
                    "flashes": list(a.flashes),
                }
            now = time.time()
            conn.execute(
                "UPDATE library_versions SET status = 'historical' WHERE status = 'active'"
            )
            cur = conn.execute(
                "INSERT INTO library_versions (status, base_version, source, created_at, activated_at) "
                "VALUES ('active', ?, 'import', ?, ?)",
                (base_id, now, now),
            )
            vid = int(cur.lastrowid)
            conn.executemany(
                "INSERT INTO library_version_aids (version_id, aid_id, period, flashes) "
                "VALUES (?, ?, ?, ?)",
                [
                    (vid, a["aid_id"], a["period"], json.dumps(a["flashes"]))
                    for a in merged.values()
                ],
            )
            conn.execute(
                "INSERT INTO library_state (id, version_id) VALUES (1, ?) "
                "ON CONFLICT(id) DO UPDATE SET version_id = excluded.version_id",
                (vid,),
            )
        return len(aids), self.get_version(vid)

    def list_versions(self) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT * FROM library_versions ORDER BY version_id"
            ).fetchall()
            return [self._version_meta(conn, r) for r in rows]

    def get_version(self, version_id: int) -> dict | None:
        with closing(self._connect()) as conn:
            row = conn.execute(
                "SELECT * FROM library_versions WHERE version_id = ?", (version_id,)
            ).fetchone()
            return self._version_meta(conn, row) if row is not None else None

    def get_version_aids(self, version_id: int) -> list[dict]:
        with closing(self._connect()) as conn:
            return self._aids_of(conn, version_id)

    def get_active_snapshot(self) -> tuple[int | None, list[dict]]:
        """/judge 请求开始时固定一次：同一读事务内取激活版本号与完整快照。"""
        with closing(self._connect()) as conn:
            try:
                conn.execute("BEGIN")
                state = conn.execute(
                    "SELECT version_id FROM library_state WHERE id = 1"
                ).fetchone()
                vid = state["version_id"] if state is not None else None
                aids = self._aids_of(conn, vid) if vid is not None else []
            finally:
                conn.rollback()  # 只读事务，释放读锁
        return vid, aids

    # ---------------- 判定记录 ----------------

    def save_judgment(
        self,
        *,
        status: str,
        best_aid_id: str | None,
        matched: int,
        missed: int,
        false_count: int,
        drift: float | None,
        library_version: int | None,
        request: dict,
        response: dict,
    ) -> int:
        with self._write_tx() as conn:
            cur = conn.execute(
                "INSERT INTO judgments "
                "(created_at, status, best_aid_id, matched, missed, false_count, drift, "
                "library_version, request, response) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    time.time(),
                    status,
                    best_aid_id,
                    matched,
                    missed,
                    false_count,
                    drift,
                    library_version,
                    json.dumps(request, ensure_ascii=False),
                    json.dumps(response, ensure_ascii=False),
                ),
            )
            return int(cur.lastrowid)

    def list_judgments(self, limit: int = 50, offset: int = 0) -> list[dict]:
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT id, created_at, status, best_aid_id, matched, missed, false_count, "
                "drift, library_version "
                "FROM judgments ORDER BY id DESC LIMIT ? OFFSET ?",
                (limit, offset),
            ).fetchall()
        return [
            {
                "id": r["id"],
                "created_at": _iso(r["created_at"]),
                "status": r["status"],
                "best_aid_id": r["best_aid_id"],
                "matched": r["matched"],
                "missed": r["missed"],
                "false": r["false_count"],
                "drift": r["drift"],
                "library_version": r["library_version"],
            }
            for r in rows
        ]

    def get_judgment(self, judgment_id: int) -> dict | None:
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT id, created_at, status, best_aid_id, matched, missed, false_count, "
                "drift, library_version, request, response "
                "FROM judgments WHERE id = ?",
                (judgment_id,),
            ).fetchone()
        if r is None:
            return None
        return {
            "id": r["id"],
            "created_at": _iso(r["created_at"]),
            "status": r["status"],
            "best_aid_id": r["best_aid_id"],
            "matched": r["matched"],
            "missed": r["missed"],
            "false": r["false_count"],
            "drift": r["drift"],
            "library_version": r["library_version"],
            "request": json.loads(r["request"]),
            "response": json.loads(r["response"]),
        }

    # ---------------- 现场设备批次链 ----------------

    def submit_batch(self, device_id: str, req) -> tuple[dict, bool]:
        """单事务提交批次：幂等/链头校验 → 追加记录/累加重放 → 更新设备游标。

        返回 ``(响应体, 是否新接受)``。相同载荷重放（摘要一致）在同一幂等事务
        内原子累加该批次 ``replay_count``、刷新其 ``last_replayed_at`` 并把设备
        游标的最近事件改为本次重放，但**不**新增批次、**不**推进链头、**不**
        改写首次受理响应；响应仍返回原结果并标记 ``replayed``。载荷变化或链头
        过期抛 :class:`BatchConflict`。写锁 + BEGIN IMMEDIATE 串行化并发提交，
        唯一约束兜底，不会形成分叉，并发重放也不会丢失计数。
        """
        digest = compute_digest(canonical_payload(device_id, req))
        prev = req.previous_digest or ""
        thresholds = {
            "tolerance": req.tolerance,
            "max_drift": req.max_drift,
            "max_missed": req.max_missed,
            "max_false": req.max_false,
        }
        observations = [
            {"local_id": o.local_id, "captured_at": o.captured_at}
            for o in req.observations
        ]
        now = time.time()
        with self._write_tx() as conn:
            row_bid = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND batch_id = ?",
                (device_id, req.batch_id),
            ).fetchone()
            row_seq = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, req.sequence),
            ).fetchone()
            chain = conn.execute(
                "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
            ).fetchone()
            last_seq = chain["last_sequence"] if chain is not None else 0
            head = chain["head_digest"] if chain is not None else ""

            if row_bid is not None or row_seq is not None:
                # 同一 batch_id 且同一 sequence 且摘要一致 → 相同载荷重放
                if (
                    row_bid is not None
                    and row_seq is not None
                    and row_bid["id"] == row_seq["id"]
                    and row_bid["digest"] == digest
                ):
                    # 幂等重放：原子累加计数并刷新批次与链游标的最近事件；
                    # 不新增记录、不推进链头、不改写首次受理响应（response 列）
                    conn.execute(
                        "UPDATE device_batches SET "
                        "replay_count = replay_count + 1, last_replayed_at = ? "
                        "WHERE id = ?",
                        (now, row_bid["id"]),
                    )
                    conn.execute(
                        "UPDATE device_chains SET "
                        "last_event_status = 'replayed', "
                        "last_event_sequence = ?, last_event_at = ? "
                        "WHERE device_id = ?",
                        (row_bid["sequence"], now, device_id),
                    )
                    body = json.loads(row_bid["response"])
                    body["status"] = "replayed"
                    return body, False
                existing = row_bid if row_bid is not None else row_seq
                raise BatchConflict(
                    reason="payload_changed",
                    message=(
                        f"batch_id/sequence already recorded with a different "
                        f"payload (existing sequence {existing['sequence']}, "
                        f"batch_id {existing['batch_id']!r})"
                    ),
                    expected_sequence=last_seq + 1,
                    expected_previous_digest=head,
                    existing={
                        "sequence": existing["sequence"],
                        "batch_id": existing["batch_id"],
                        "digest": existing["digest"],
                    },
                )

            if req.sequence != last_seq + 1 or prev != head:
                raise BatchConflict(
                    reason="stale_chain_head",
                    message=(
                        f"batch does not extend the chain head: got sequence "
                        f"{req.sequence} with previous_digest {prev!r}, expected "
                        f"sequence {last_seq + 1} with previous_digest {head!r}"
                    ),
                    expected_sequence=last_seq + 1,
                    expected_previous_digest=head,
                )

            body = {
                "status": "accepted",
                "device_id": device_id,
                "batch_id": req.batch_id,
                "sequence": req.sequence,
                "digest": digest,
                "previous_digest": prev,
                "observation_count": len(observations),
                "received_at": _iso(now),
                "chain": {
                    "head_sequence": req.sequence,
                    "head_digest": digest,
                    "next_sequence": req.sequence + 1,
                    "expected_previous_digest": digest,
                },
            }
            conn.execute(
                "INSERT INTO device_batches "
                "(device_id, sequence, batch_id, previous_digest, digest, "
                "observation_count, thresholds, observations, response, status, "
                "created_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 'accepted', ?)",
                (
                    device_id,
                    req.sequence,
                    req.batch_id,
                    prev,
                    digest,
                    len(observations),
                    json.dumps(thresholds, ensure_ascii=False),
                    json.dumps(observations, ensure_ascii=False),
                    json.dumps(body, ensure_ascii=False),
                    now,
                ),
            )
            conn.execute(
                "INSERT INTO device_chains "
                "(device_id, last_sequence, head_digest, batch_count, updated_at, "
                "last_event_status, last_event_sequence, last_event_at) "
                "VALUES (?, ?, ?, 1, ?, 'accepted', ?, ?) "
                "ON CONFLICT(device_id) DO UPDATE SET "
                "last_sequence = excluded.last_sequence, "
                "head_digest = excluded.head_digest, "
                "batch_count = device_chains.batch_count + 1, "
                "updated_at = excluded.updated_at, "
                "last_event_status = 'accepted', "
                "last_event_sequence = excluded.last_event_sequence, "
                "last_event_at = excluded.last_event_at",
                (device_id, req.sequence, digest, now, req.sequence, now),
            )
        return body, True

    def get_device_cursor(self, device_id: str) -> dict:
        """设备批次链游标：链头位置、下一批期望值与最近一次事件（受理/重放）。

        ``last_event_status`` 为最近事件的 ``accepted``/``replayed``，
        ``last_event_sequence`` 为该事件所属批次序号，``last_event_at`` 为其
        时刻；未上传过的设备三者分别为 ``None``/``0``/``None``。
        """
        with closing(self._connect()) as conn:
            chain = conn.execute(
                "SELECT * FROM device_chains WHERE device_id = ?", (device_id,)
            ).fetchone()
        if chain is None:
            return {
                "device_id": device_id,
                "last_sequence": 0,
                "head_digest": "",
                "batch_count": 0,
                "next_sequence": 1,
                "expected_previous_digest": "",
                "updated_at": None,
                "last_event_status": None,
                "last_event_sequence": 0,
                "last_event_at": None,
            }
        return {
            "device_id": device_id,
            "last_sequence": chain["last_sequence"],
            "head_digest": chain["head_digest"],
            "batch_count": chain["batch_count"],
            "next_sequence": chain["last_sequence"] + 1,
            "expected_previous_digest": chain["head_digest"],
            "updated_at": _iso(chain["updated_at"]),
            "last_event_status": chain["last_event_status"],
            "last_event_sequence": chain["last_event_sequence"],
            "last_event_at": _iso(chain["last_event_at"]),
        }

    def list_device_batches(
        self, device_id: str, *, after_sequence: int = 0, limit: int = 50
    ) -> dict:
        """按 sequence 游标分页列出设备批次（升序）。

        每条返回首次受理事实 ``status='accepted'`` 及与详情一致的
        ``replay_count`` / ``last_replayed_at``。
        """
        with closing(self._connect()) as conn:
            rows = conn.execute(
                "SELECT sequence, batch_id, previous_digest, digest, "
                "observation_count, status, replay_count, last_replayed_at, created_at "
                "FROM device_batches "
                "WHERE device_id = ? AND sequence > ? ORDER BY sequence LIMIT ?",
                (device_id, after_sequence, limit + 1),
            ).fetchall()
        has_more = len(rows) > limit
        rows = rows[:limit]
        items = [
            {
                "sequence": r["sequence"],
                "batch_id": r["batch_id"],
                "previous_digest": r["previous_digest"],
                "digest": r["digest"],
                "observation_count": r["observation_count"],
                "status": r["status"],
                "replay_count": r["replay_count"],
                "last_replayed_at": _iso(r["last_replayed_at"]),
                "created_at": _iso(r["created_at"]),
            }
            for r in rows
        ]
        return {
            "device_id": device_id,
            "count": len(items),
            "batches": items,
            "next_cursor": items[-1]["sequence"] if has_more and items else None,
        }

    def get_device_batch(self, device_id: str, sequence: int) -> dict | None:
        """单条批次详情：观测时间戳、/judge 四项阈值、摘要、首次受理状态与重放信息。"""
        with closing(self._connect()) as conn:
            r = conn.execute(
                "SELECT * FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, sequence),
            ).fetchone()
        if r is None:
            return None
        return {
            "status": r["status"],
            "replay_count": r["replay_count"],
            "last_replayed_at": _iso(r["last_replayed_at"]),
            "device_id": r["device_id"],
            "sequence": r["sequence"],
            "batch_id": r["batch_id"],
            "digest": r["digest"],
            "previous_digest": r["previous_digest"],
            "observation_count": r["observation_count"],
            "observations": json.loads(r["observations"]),
            "thresholds": json.loads(r["thresholds"]),
            "created_at": _iso(r["created_at"]),
        }
