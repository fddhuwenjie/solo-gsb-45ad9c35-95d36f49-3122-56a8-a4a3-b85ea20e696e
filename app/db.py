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

批次多处理方确认（稀疏确认 + 连续水位）：

- ``device_consumers``：每台设备登记的必需消费者（下游处理方），登记永久
  有效、不可删除；``watermark`` 持久化该消费者已连续确认到的水位
  （``1..watermark`` 均已确认），新登记消费者水位为 0；
- ``device_batch_acks``：每个消费者的稀疏确认记录（缺号允许），携带确认时
  的批次摘要与确认时刻，``(device_id, consumer_id, sequence)`` 唯一。

批次确认在单个事务内完成「消费者/批次存在性与摘要校验 → 落稀疏确认 →
由稀疏集合重算并推进连续水位」：允许乱序、并发与重复提交，相同确认幂等，
摘要冲突返回 409。写锁 + BEGIN IMMEDIATE 串行化并发确认，水位始终由
``device_batch_acks`` 稀疏集合重新推导（而非读旧值 +1），乱序补缺时水位
只进不退，并发确认不会丢失推进。设备的 ``safe_sequence`` 取全部必需消费者
水位的最小值；尚无消费者或任一消费者未确认前缀时为 0。

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
CREATE TABLE IF NOT EXISTS device_consumers (
    device_id    TEXT NOT NULL,
    consumer_id  TEXT NOT NULL,
    watermark    INTEGER NOT NULL DEFAULT 0,
    registered_at REAL NOT NULL,
    PRIMARY KEY (device_id, consumer_id)
);
CREATE TABLE IF NOT EXISTS device_batch_acks (
    device_id   TEXT NOT NULL,
    consumer_id TEXT NOT NULL,
    sequence    INTEGER NOT NULL,
    digest      TEXT NOT NULL,
    acked_at    REAL NOT NULL,
    PRIMARY KEY (device_id, consumer_id, sequence)
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


class ConsumerNotFound(Exception):
    """设备的必需消费者未登记（HTTP 404）。登记永久有效，不可删除。"""


class ConfirmationTargetNotFound(Exception):
    """确认的批次不存在（HTTP 404）：仅接受已存在的批次。"""


class AckConflict(Exception):
    """批次确认冲突（HTTP 409）：批次摘要不符，detail 附期望摘要。"""

    def __init__(
        self,
        *,
        reason: str,
        message: str,
        expected_digest: str,
        existing_digest: str | None = None,
    ):
        super().__init__(message)
        self.detail = {
            "error": "ack_conflict",
            "reason": reason,
            "message": message,
            "expected_digest": expected_digest,
        }
        if existing_digest is not None:
            self.detail["existing_digest"] = existing_digest


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

    # ---------------- 设备批次多处理方确认 ----------------

    @staticmethod
    def _advance_watermark(conn, device_id: str, consumer_id: str) -> int:
        """在写事务内由稀疏确认集合重算连续水位并持久化，返回新水位。

        稀疏集合是事实源：取 ``1..max(sequence)`` 中最大的无缺口连续前缀。
        每次确认后都重新推导（而非读旧水位 +1），因此乱序补缺（先确认 2、3
        再补 1）水位会一次跃过缺口，重复/越序确认不会回退——并发确认由外层
        写锁串行化，提交的水位不会丢失推进。
        """
        rows = conn.execute(
            "SELECT sequence FROM device_batch_acks "
            "WHERE device_id = ? AND consumer_id = ? ORDER BY sequence",
            (device_id, consumer_id),
        ).fetchall()
        watermark = 0
        for r in rows:
            if r["sequence"] != watermark + 1:
                break
            watermark = r["sequence"]
        conn.execute(
            "UPDATE device_consumers SET watermark = ? "
            "WHERE device_id = ? AND consumer_id = ?",
            (watermark, device_id, consumer_id),
        )
        return watermark

    @staticmethod
    def _safe_sequence(watermarks: list[int]) -> int:
        """``safe_sequence`` 取全部必需消费者水位最小值；无消费者时为 0。"""
        return min(watermarks) if watermarks else 0

    def register_consumer(self, device_id: str, consumer_id: str) -> tuple[dict, bool]:
        """登记设备的必需消费者；登记永久有效、不可删除，重复登记幂等。

        返回 ``(响应体, 是否新登记)``：新登记 HTTP 201
        （``status=registered``），重复登记 HTTP 200
        （``status=already_registered``），保留首次登记时刻与已有水位。
        新消费者水位为 0，登记后设备 ``safe_sequence`` 立即取包括它在内的
        全部水位最小值（其未确认前缀期间为 0）。
        """
        now = time.time()
        with self._write_tx() as conn:
            existing = conn.execute(
                "SELECT registered_at, watermark FROM device_consumers "
                "WHERE device_id = ? AND consumer_id = ?",
                (device_id, consumer_id),
            ).fetchone()
            if existing is None:
                conn.execute(
                    "INSERT INTO device_consumers "
                    "(device_id, consumer_id, watermark, registered_at) "
                    "VALUES (?, ?, 0, ?)",
                    (device_id, consumer_id, now),
                )
                registered_at = now
                watermark = 0
                ack_status = "registered"
            else:
                registered_at = existing["registered_at"]
                watermark = existing["watermark"]
                ack_status = "already_registered"
            rows = conn.execute(
                "SELECT watermark FROM device_consumers "
                "WHERE device_id = ? ORDER BY consumer_id",
                (device_id,),
            ).fetchall()
            watermarks = [r["watermark"] for r in rows]
            safe_sequence = self._safe_sequence(watermarks)
            consumer_count = len(rows)
        body = {
            "status": ack_status,
            "device_id": device_id,
            "consumer_id": consumer_id,
            "watermark": watermark,
            "registered_at": _iso(registered_at),
            "safe_sequence": safe_sequence,
            "consumer_count": consumer_count,
        }
        return body, ack_status == "registered"

    def confirm_batch(
        self, device_id: str, consumer_id: str, sequence: int, batch_digest: str
    ) -> tuple[dict, bool]:
        """单事务确认批次：校验 → 落稀疏确认 → 推进连续水位，支持乱序/并发/重复。

        - 消费者未登记 → :class:`ConsumerNotFound`（404）；
        - 批次不存在 → :class:`ConfirmationTargetNotFound`（404），
          仅接受已存在批次；
        - 批次存在但提交摘要不符 → :class:`AckConflict`（409，附期望摘要）；
        - 同一 ``(消费者, 序号)`` 已以相同摘要确认 → 幂等
          （``already_confirmed``，HTTP 200），不写任何状态；以不同摘要重复
          确认 → 409（附既有摘要与期望摘要）。

        新确认（HTTP 201，``confirmed``）在同一事务内保存稀疏确认并把连续
        水位重算推进到首个缺口；返回该消费者水位与设备 ``safe_sequence``
        （全部水位最小值）。
        """
        now = time.time()
        with self._write_tx() as conn:
            consumer = conn.execute(
                "SELECT watermark FROM device_consumers "
                "WHERE device_id = ? AND consumer_id = ?",
                (device_id, consumer_id),
            ).fetchone()
            if consumer is None:
                raise ConsumerNotFound(
                    f"consumer {consumer_id!r} is not registered for device "
                    f"{device_id!r}; register it first "
                    f"(POST /field-devices/{device_id}/consumers)"
                )
            batch = conn.execute(
                "SELECT digest FROM device_batches WHERE device_id = ? AND sequence = ?",
                (device_id, sequence),
            ).fetchone()
            if batch is None:
                raise ConfirmationTargetNotFound(
                    f"batch {sequence} not found for device {device_id!r}; "
                    "only existing batches can be confirmed"
                )
            expected = batch["digest"]
            existing_ack = conn.execute(
                "SELECT digest, acked_at FROM device_batch_acks "
                "WHERE device_id = ? AND consumer_id = ? AND sequence = ?",
                (device_id, consumer_id, sequence),
            ).fetchone()
            if existing_ack is not None:
                if existing_ack["digest"] == batch_digest:
                    # 幂等：相同确认不写任何状态。水位直接读当前持久值
                    # （乱序补缺的其他事务可能已把水位推进过），不再 UPDATE
                    acked_at = existing_ack["acked_at"]
                    watermark = consumer["watermark"]
                    is_new = False
                    ack_status = "already_confirmed"
                else:
                    raise AckConflict(
                        reason="ack_digest_conflict",
                        message=(
                            f"consumer {consumer_id!r} already confirmed batch "
                            f"{sequence} with a different digest; confirmation is "
                            "immutable"
                        ),
                        expected_digest=expected,
                        existing_digest=existing_ack["digest"],
                    )
            elif batch_digest != expected:
                raise AckConflict(
                    reason="batch_digest_mismatch",
                    message=(
                        f"batch_digest does not match stored batch {sequence}: "
                        f"got {batch_digest!r}, expected {expected!r}"
                    ),
                    expected_digest=expected,
                )
            else:
                conn.execute(
                    "INSERT INTO device_batch_acks "
                    "(device_id, consumer_id, sequence, digest, acked_at) "
                    "VALUES (?, ?, ?, ?, ?)",
                    (device_id, consumer_id, sequence, expected, now),
                )
                acked_at = now
                is_new = True
                ack_status = "confirmed"
                # 稀疏确认与水位推进在同一事务；水位由稀疏集合重算，并发不丢推进
                watermark = self._advance_watermark(conn, device_id, consumer_id)
            body = self._ack_status_body(
                conn,
                device_id,
                confirming_consumer=consumer_id,
                ack_status=ack_status,
                sequence=sequence,
                digest=batch_digest,
                acked_at=acked_at,
                watermark=watermark,
            )
        return body, is_new

    def get_consumer_status(self, device_id: str) -> dict:
        """设备多处理方确认状态：各消费者水位、各/总首个缺口与阻塞消费者。

        - ``consumers[].watermark``：每个必需消费者的连续确认水位
          （``1..watermark`` 全部确认），``first_gap`` 为其首个缺口
          （``watermark + 1``），``blocking`` 标注该消费者是否阻塞
          ``safe_sequence``（水位尚未追平链头，即其首个缺口是已存在批次）；
        - ``first_gap``：各消费者首个缺口的最小值，即所有人共同缺的第一批；
        - ``blocking_consumers``：阻塞 ``safe_sequence`` 的消费者编号；
        - ``safe_sequence``：全部水位最小值（无消费者或任一消费者未确认前
          缀时为 0），即全部处理完成、可安全归档的连续前缀。
        """
        with closing(self._connect()) as conn:
            return self._ack_status_body(conn, device_id)

    def _ack_status_body(
        self,
        conn,
        device_id: str,
        *,
        confirming_consumer: str | None = None,
        ack_status: str | None = None,
        sequence: int | None = None,
        digest: str | None = None,
        acked_at: float | None = None,
        watermark: int | None = None,
    ) -> dict:
        """组装确认状态视图（调用方置于读连接或写事务内，保证读到一致视图）。

        既被写事务内的 :meth:`confirm_batch` 复用，也被只读的
        :meth:`get_consumer_status` 复用：消费者水位、链头在同一连接内读出，
        写事务内则含本次未提交即可见的更新。
        """
        chain = conn.execute(
            "SELECT last_sequence FROM device_chains WHERE device_id = ?",
            (device_id,),
        ).fetchone()
        head = chain["last_sequence"] if chain is not None else 0
        rows = conn.execute(
            "SELECT consumer_id, watermark, registered_at FROM device_consumers "
            "WHERE device_id = ? ORDER BY consumer_id",
            (device_id,),
        ).fetchall()
        consumers = []
        watermarks: list[int] = []
        for r in rows:
            wm = r["watermark"]
            if confirming_consumer is not None and r["consumer_id"] == confirming_consumer:
                wm = watermark if watermark is not None else wm
            gap = wm + 1
            # 首个缺口落在已存在批次上才算阻塞；已追平链头（缺口尚无批次）
            # 则不阻塞归档
            blocking = gap <= head
            consumers.append(
                {
                    "consumer_id": r["consumer_id"],
                    "watermark": wm,
                    "first_gap": gap,
                    "blocking": blocking,
                    "registered_at": _iso(r["registered_at"]),
                }
            )
            watermarks.append(wm)
        safe_sequence = self._safe_sequence(watermarks)
        first_gap = min((c["first_gap"] for c in consumers), default=1)
        blocking_consumers = [c["consumer_id"] for c in consumers if c["blocking"]]
        body: dict = {
            "device_id": device_id,
            "head_sequence": head,
            "safe_sequence": safe_sequence,
            "first_gap": first_gap,
            "blocking_consumers": blocking_consumers,
            "consumer_count": len(consumers),
            "consumers": consumers,
        }
        if confirming_consumer is not None:
            body["status"] = ack_status
            body["consumer_id"] = confirming_consumer
            body["sequence"] = sequence
            body["batch_digest"] = digest
            body["watermark"] = watermark
            body["acked_at"] = _iso(acked_at)
        return body
