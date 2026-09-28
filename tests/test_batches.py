"""现场设备批次链接口测试：链式追加、摘要、幂等重放、冲突与并发。"""
import hashlib
import json
import os
import sys
import threading

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient
from pydantic import ValidationError

from app.main import create_app
from app.models import BatchUploadRequest

THRESHOLDS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}
DEVICE = "dev-01"


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def obs(n, base=1_700_000_000.0, step=0.5):
    return [
        {"local_id": f"o{i:03d}", "captured_at": base + i * step}
        for i in range(n)
    ]


def batch(batch_id, sequence, previous_digest="", observations=None, **kw):
    body = {
        "batch_id": batch_id,
        "sequence": sequence,
        "previous_digest": previous_digest,
        "observations": observations if observations is not None else obs(3),
        **THRESHOLDS,
    }
    body.update(kw)
    return body


def expected_digest(device_id, body):
    """按约定规则独立复算批次摘要：排除摘要字段，UTF-8 键序紧凑 JSON 的 SHA-256。"""
    payload = {
        "batch_id": body["batch_id"],
        "device_id": device_id,
        "max_drift": body["max_drift"],
        "max_false": body["max_false"],
        "max_missed": body["max_missed"],
        "observations": [
            {"captured_at": o["captured_at"], "local_id": o["local_id"]}
            for o in body["observations"]
        ],
        "sequence": body["sequence"],
        "tolerance": body["tolerance"],
    }
    blob = json.dumps(
        payload, ensure_ascii=False, sort_keys=True, separators=(",", ":")
    ).encode("utf-8")
    return hashlib.sha256(blob).hexdigest()


def upload(client, body, device=DEVICE):
    return client.post(f"/field-devices/{device}/batches", json=body)


# ---------------- 链式追加与摘要 ----------------


def test_first_batch_accepted_and_digest(client):
    body = batch("b-1", 1)
    r = upload(client, body)
    assert r.status_code == 201
    resp = r.json()
    assert resp["status"] == "accepted"
    assert resp["device_id"] == DEVICE
    assert resp["batch_id"] == "b-1" and resp["sequence"] == 1
    assert resp["previous_digest"] == ""
    assert resp["observation_count"] == 3
    # 摘要与服务端约定一致：排除 previous_digest，UTF-8 键序紧凑 JSON 的 SHA-256
    assert resp["digest"] == expected_digest(DEVICE, body)
    assert resp["chain"] == {
        "head_sequence": 1,
        "head_digest": resp["digest"],
        "next_sequence": 2,
        "expected_previous_digest": resp["digest"],
    }

    # 游标反映链头
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 1
    assert cur["head_digest"] == resp["digest"]
    assert cur["next_sequence"] == 2
    assert cur["expected_previous_digest"] == resp["digest"]
    assert cur["batch_count"] == 1


def test_first_batch_requires_empty_previous_digest(client):
    body = batch("b-1", 1, previous_digest="ab" * 32)
    r = upload(client, body)
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "stale_chain_head"
    assert detail["expected_sequence"] == 1
    assert detail["expected_previous_digest"] == ""


def test_chain_append_and_stale_head(client):
    r1 = upload(client, batch("b-1", 1))
    d1 = r1.json()["digest"]

    # 链头过期：previous_digest 不是当前链头
    r = upload(client, batch("b-2", 2, previous_digest="00" * 32))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "stale_chain_head"
    assert detail["expected_sequence"] == 2
    assert detail["expected_previous_digest"] == d1

    # 正确引用链头 → 接受
    r2 = upload(client, batch("b-2", 2, previous_digest=d1))
    assert r2.status_code == 201
    assert r2.json()["previous_digest"] == d1

    # 再次用旧链头 → 409，期望值已前移
    r = upload(client, batch("b-3", 3, previous_digest=d1))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["expected_sequence"] == 3
    assert detail["expected_previous_digest"] == r2.json()["digest"]


def test_sequence_must_extend_head(client):
    upload(client, batch("b-1", 1))
    d1 = client.get(f"/field-devices/{DEVICE}/cursor").json()["head_digest"]
    # 跳号：sequence 不是链头 + 1
    r = upload(client, batch("b-3", 3, previous_digest=d1))
    assert r.status_code == 409
    assert r.json()["detail"]["expected_sequence"] == 2


def test_digest_covers_payload_and_device(client):
    # 同一载荷在不同设备上摘要不同（device_id 参与摘要）
    body = batch("b-1", 1)
    r_a = upload(client, body, device="dev-a")
    r_b = upload(client, body, device="dev-b")
    assert r_a.status_code == 201 and r_b.status_code == 201
    assert r_a.json()["digest"] != r_b.json()["digest"]
    assert r_a.json()["digest"] == expected_digest("dev-a", body)

    # 非 ASCII 内容按 UTF-8 参与摘要（ensure_ascii=False）
    body2 = batch("b-2", 1, observations=[
        {"local_id": "浮标-甲", "captured_at": 1_700_000_000.0},
        {"local_id": "浮标-乙", "captured_at": 1_700_000_001.0},
    ])
    r = upload(client, body2, device="dev-c")
    assert r.status_code == 201
    assert r.json()["digest"] == expected_digest("dev-c", body2)


# ---------------- 幂等重放与冲突 ----------------


def test_replay_same_payload_returns_original(client):
    body = batch("b-1", 1)
    r1 = upload(client, body)
    assert r1.status_code == 201
    original = r1.json()

    r2 = upload(client, body)
    assert r2.status_code == 200
    replayed = r2.json()
    assert replayed["status"] == "replayed"
    # 原结果内容不变（仅状态标记为 replayed）
    assert {k: v for k, v in replayed.items() if k != "status"} == {
        k: v for k, v in original.items() if k != "status"
    }

    # 链头与记录数不变
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 1 and cur["batch_count"] == 1
    assert client.get(f"/field-devices/{DEVICE}/batches").json()["count"] == 1


def test_same_batch_id_different_payload_conflict(client):
    r1 = upload(client, batch("b-1", 1))
    assert r1.status_code == 201
    d1 = r1.json()["digest"]

    # 同 batch_id、不同观测 → 409，附既有批次与期望值
    r = upload(client, batch("b-1", 2, previous_digest=d1, observations=obs(4)))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "payload_changed"
    assert detail["existing_batch"] == {
        "sequence": 1, "batch_id": "b-1", "digest": d1,
    }
    assert detail["expected_sequence"] == 2
    assert detail["expected_previous_digest"] == d1

    # 同 batch_id、不同序号 → 409
    r = upload(client, batch("b-1", 2, previous_digest=d1))
    assert r.status_code == 409
    assert r.json()["detail"]["reason"] == "payload_changed"


def test_same_sequence_different_payload_conflict(client):
    r1 = upload(client, batch("b-1", 1))
    assert r1.status_code == 201

    # 同 sequence、不同 batch_id → 409
    r = upload(client, batch("b-x", 1))
    assert r.status_code == 409
    detail = r.json()["detail"]
    assert detail["reason"] == "payload_changed"
    assert detail["existing_batch"]["batch_id"] == "b-1"
    assert detail["existing_batch"]["sequence"] == 1


def test_replay_after_chain_advanced(client):
    """链头前移后重放历史批次：相同载荷仍返回原结果，不按链头过期处理。"""
    r1 = upload(client, batch("b-1", 1))
    d1 = r1.json()["digest"]
    upload(client, batch("b-2", 2, previous_digest=d1))

    r = upload(client, batch("b-1", 1))
    assert r.status_code == 200
    assert r.json()["status"] == "replayed"
    assert r.json()["digest"] == d1


def test_sequential_replays_persisted_and_reflect_cursor(client):
    """顺序重放：列表/详情保留首次受理事实，重放计数与游标最近事件持久化。"""
    r1 = upload(client, batch("b-1", 1))
    original1 = r1.json()
    d1 = original1["digest"]
    r2 = upload(client, batch("b-2", 2, previous_digest=d1))
    d2 = r2.json()["digest"]

    # 首次重放 seq1：POST 200 replayed，原响应不被改写
    rp1 = upload(client, batch("b-1", 1))
    assert rp1.status_code == 200 and rp1.json()["status"] == "replayed"
    assert {k: v for k, v in rp1.json().items() if k != "status"} == {
        k: v for k, v in original1.items() if k != "status"
    }

    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    # 重放不推进链头、不新增批次
    assert cur["last_sequence"] == 2 and cur["batch_count"] == 2
    assert cur["head_digest"] == d2
    # 游标最近事件改为该次重放（序号是重放批次自己的序号，不是链头）
    assert cur["last_event_status"] == "replayed"
    assert cur["last_event_sequence"] == 1
    assert cur["last_event_at"] is not None

    # 列表/详情仍保留 status=accepted（首次受理事实），另带 replay_count
    lst = client.get(f"/field-devices/{DEVICE}/batches").json()["batches"]
    by_seq = {b["sequence"]: b for b in lst}
    assert by_seq[1]["status"] == "accepted" and by_seq[1]["replay_count"] == 1
    assert by_seq[1]["last_replayed_at"] is not None
    assert by_seq[2]["replay_count"] == 0 and by_seq[2]["last_replayed_at"] is None
    detail1 = client.get(f"/field-devices/{DEVICE}/batches/1").json()
    assert detail1["status"] == "accepted"
    assert detail1["replay_count"] == 1
    assert detail1["last_replayed_at"] == by_seq[1]["last_replayed_at"]

    # 再次重放 seq1：计数原子累加到 2
    rp2 = upload(client, batch("b-1", 1))
    assert rp2.status_code == 200 and rp2.json()["status"] == "replayed"
    detail1 = client.get(f"/field-devices/{DEVICE}/batches/1").json()
    assert detail1["replay_count"] == 2

    # 改重放链头批次 seq2：游标最近事件序号随之变为 2（链头仍不动）
    upload(client, batch("b-2", 2, previous_digest=d1))
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_event_status"] == "replayed"
    assert cur["last_event_sequence"] == 2
    assert cur["last_sequence"] == 2 and cur["batch_count"] == 2
    detail2 = client.get(f"/field-devices/{DEVICE}/batches/2").json()
    assert detail2["status"] == "accepted" and detail2["replay_count"] == 1

    # 新批次受理后：链头推进，游标最近事件改回 accepted；历史重放计数保留
    r3 = upload(client, batch("b-3", 3, previous_digest=d2))
    assert r3.status_code == 201 and r3.json()["status"] == "accepted"
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 3 and cur["batch_count"] == 3
    assert cur["head_digest"] == r3.json()["digest"]
    assert cur["last_event_status"] == "accepted"
    assert cur["last_event_sequence"] == 3
    assert cur["last_event_at"] == cur["updated_at"]
    detail1 = client.get(f"/field-devices/{DEVICE}/batches/1").json()
    assert detail1["replay_count"] == 2
    detail2 = client.get(f"/field-devices/{DEVICE}/batches/2").json()
    assert detail2["replay_count"] == 1
    detail3 = client.get(f"/field-devices/{DEVICE}/batches/3").json()
    assert detail3["replay_count"] == 0 and detail3["last_replayed_at"] is None


# ---------------- 请求校验 ----------------


def test_validation_errors(client):
    # 观测为空 / 超过 100 条
    assert upload(client, batch("b-1", 1, observations=[])).status_code == 422
    assert upload(client, batch("b-1", 1, observations=obs(101))).status_code == 422

    # 未按 (captured_at, local_id) 递增
    unsorted = [
        {"local_id": "a", "captured_at": 2.0},
        {"local_id": "b", "captured_at": 1.0},
    ]
    assert upload(client, batch("b-1", 1, observations=unsorted)).status_code == 422
    # captured_at 相同但 local_id 逆序
    tie_unsorted = [
        {"local_id": "b", "captured_at": 1.0},
        {"local_id": "a", "captured_at": 1.0},
    ]
    assert upload(client, batch("b-1", 1, observations=tie_unsorted)).status_code == 422
    # (captured_at, local_id) 完全重复
    dup = [
        {"local_id": "a", "captured_at": 1.0},
        {"local_id": "a", "captured_at": 1.0},
    ]
    assert upload(client, batch("b-1", 1, observations=dup)).status_code == 422
    # captured_at 相同、local_id 递增 → 合法
    tie_ok = [
        {"local_id": "a", "captured_at": 1.0},
        {"local_id": "b", "captured_at": 1.0},
    ]
    assert upload(client, batch("b-1", 1, observations=tie_ok)).status_code == 201

    # previous_digest 格式非法 / sequence 非法 / 阈值非法
    assert upload(client, batch("b-2", 1, previous_digest="xyz")).status_code == 422
    assert upload(client, batch("b-2", 0)).status_code == 422
    assert upload(client, batch("b-2", 1, tolerance=0)).status_code == 422
    assert upload(client, batch("b-2", 1, max_drift=1.0)).status_code == 422
    assert upload(client, batch("b-2", 1, max_missed=-1)).status_code == 422
    assert upload(client, batch("b-2", 1, max_false=-1)).status_code == 422

    # captured_at 非有限值：模型层拒绝（NaN/Inf 经 HTTP 提交时与原 API 一致，
    # 由 FastAPI 默认校验错误处理返回，不在本接口新增特殊行为）
    with pytest.raises(ValidationError):
        BatchUploadRequest(
            batch_id="b-2",
            sequence=1,
            observations=[{"local_id": "a", "captured_at": float("nan")}],
            **THRESHOLDS,
        )


def test_exactly_100_observations_accepted(client):
    r = upload(client, batch("b-1", 1, observations=obs(100)))
    assert r.status_code == 201
    assert r.json()["observation_count"] == 100


# ---------------- 游标与批次查询 ----------------


def test_cursor_and_batch_queries(client):
    # 未上传过的设备：空链游标（无最近事件）
    cur = client.get("/field-devices/dev-empty/cursor").json()
    assert cur["last_sequence"] == 0 and cur["head_digest"] == ""
    assert cur["next_sequence"] == 1 and cur["expected_previous_digest"] == ""
    assert cur["batch_count"] == 0 and cur["updated_at"] is None
    assert cur["last_event_status"] is None
    assert cur["last_event_sequence"] == 0
    assert cur["last_event_at"] is None
    assert client.get("/field-devices/dev-empty/batches").json()["count"] == 0

    d = upload(client, batch("b-1", 1)).json()["digest"]
    d = upload(client, batch("b-2", 2, previous_digest=d)).json()["digest"]
    upload(client, batch("b-3", 3, previous_digest=d))

    # 新批次受理后：游标最近事件为最新批次的 accepted
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_event_status"] == "accepted"
    assert cur["last_event_sequence"] == 3
    assert cur["last_event_at"] == cur["updated_at"]

    # 列表：状态保留首次受理事实 accepted，另带重放计数（无重放时为零）；
    # next_cursor 为 None 表示无更多
    lst = client.get(f"/field-devices/{DEVICE}/batches").json()
    assert lst["count"] == 3 and lst["next_cursor"] is None
    assert [b["sequence"] for b in lst["batches"]] == [1, 2, 3]
    assert all(b["status"] == "accepted" for b in lst["batches"])
    assert all(b["replay_count"] == 0 for b in lst["batches"])
    assert all(b["last_replayed_at"] is None for b in lst["batches"])
    assert lst["batches"][0]["batch_id"] == "b-1"

    # 游标分页：after_sequence 续查
    page1 = client.get(f"/field-devices/{DEVICE}/batches?limit=2").json()
    assert [b["sequence"] for b in page1["batches"]] == [1, 2]
    assert page1["next_cursor"] == 2
    page2 = client.get(
        f"/field-devices/{DEVICE}/batches?after_sequence={page1['next_cursor']}&limit=2"
    ).json()
    assert [b["sequence"] for b in page2["batches"]] == [3]
    assert page2["next_cursor"] is None

    # 详情：观测时间戳与 /judge 四项阈值均已保存，未做判定
    detail = client.get(f"/field-devices/{DEVICE}/batches/2").json()
    assert detail["status"] == "accepted"
    assert detail["replay_count"] == 0 and detail["last_replayed_at"] is None
    assert detail["observations"] == obs(3)
    assert detail["thresholds"] == THRESHOLDS
    assert detail["observation_count"] == 3
    assert "best" not in detail and "candidates" not in detail

    # 不存在 → 404
    assert client.get(f"/field-devices/{DEVICE}/batches/99").status_code == 404
    assert client.get("/field-devices/dev-empty/batches/1").status_code == 404


# ---------------- 并发：不得形成分叉 ----------------


def test_concurrent_same_sequence_no_fork(client):
    """多客户端并发提交同一序号的不同批次：恰好一个被接受，其余 409。"""
    upload(client, batch("b-1", 1))
    d1 = client.get(f"/field-devices/{DEVICE}/cursor").json()["head_digest"]

    results = []
    lock = threading.Lock()

    def submit(i):
        r = upload(client, batch(f"race-{i}", 2, previous_digest=d1))
        with lock:
            results.append(r.status_code)

    threads = [threading.Thread(target=submit, args=(i,)) for i in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(201) == 1
    assert results.count(409) == len(threads) - 1

    # 链上该序号只有一条记录，链头唯一
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 2 and cur["batch_count"] == 2
    lst = client.get(f"/field-devices/{DEVICE}/batches").json()["batches"]
    assert [b["sequence"] for b in lst] == [1, 2]


def test_concurrent_identical_replay(client):
    """断网重连后并发重发同一批次：恰好一个 accepted，其余 replayed。"""
    body = batch("b-1", 1)
    results = []
    lock = threading.Lock()

    def submit():
        r = upload(client, body)
        with lock:
            results.append((r.status_code, r.json()["status"]))

    threads = [threading.Thread(target=submit) for _ in range(8)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count((201, "accepted")) == 1
    assert results.count((200, "replayed")) == len(threads) - 1
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 1 and cur["batch_count"] == 1


def test_concurrent_replays_lose_no_count(client):
    """并发重放已受理批次：每次重放都原子累加，不得丢计数，也不得新增批次。"""
    body = batch("b-1", 1)
    r1 = upload(client, body)
    assert r1.status_code == 201

    n = 12
    barrier = threading.Barrier(n)
    results = []
    lock = threading.Lock()

    def submit():
        barrier.wait()  # 尽量让所有请求同时进入
        r = upload(client, body)
        with lock:
            results.append(r.status_code)

    threads = [threading.Thread(target=submit) for _ in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    assert results.count(200) == n

    detail = client.get(f"/field-devices/{DEVICE}/batches/1").json()
    assert detail["status"] == "accepted"  # 首次受理事实不变
    assert detail["replay_count"] == n     # 每次重放都计数，无丢失
    assert detail["last_replayed_at"] is not None

    # 不新增批次、不推进链头；游标最近事件为最后一次重放
    lst = client.get(f"/field-devices/{DEVICE}/batches").json()
    assert lst["count"] == 1 and lst["batches"][0]["replay_count"] == n
    cur = client.get(f"/field-devices/{DEVICE}/cursor").json()
    assert cur["last_sequence"] == 1 and cur["batch_count"] == 1
    assert cur["head_digest"] == r1.json()["digest"]
    assert cur["last_event_status"] == "replayed"
    assert cur["last_event_sequence"] == 1


def test_legacy_device_db_upgrade_zero_replay_count(tmp_path):
    """旧库升级：补重放列（旧记录计数为零）与游标最近事件，升级后可正常重放。"""
    import json as _json
    import sqlite3

    db = tmp_path / "legacy-devices.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE device_chains (
            device_id     TEXT PRIMARY KEY,
            last_sequence INTEGER NOT NULL,
            head_digest   TEXT NOT NULL,
            batch_count   INTEGER NOT NULL,
            updated_at    REAL NOT NULL
        );
        CREATE TABLE device_batches (
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
            created_at        REAL NOT NULL,
            UNIQUE (device_id, sequence),
            UNIQUE (device_id, batch_id)
        );
        """
    )
    # 按当前摘要规则造一条旧库批次（seq1，已受理）
    body1 = batch("b-1", 1)
    d1 = expected_digest(DEVICE, body1)
    ts1 = 1_700_000_000.0
    observations1 = body1["observations"]
    thresholds1 = {k: body1[k] for k in ("tolerance", "max_drift", "max_missed", "max_false")}
    response1 = {
        "status": "accepted",
        "device_id": DEVICE,
        "batch_id": "b-1",
        "sequence": 1,
        "digest": d1,
        "previous_digest": "",
        "observation_count": len(observations1),
        "received_at": "2023-11-14T22:13:20+00:00",
        "chain": {
            "head_sequence": 1,
            "head_digest": d1,
            "next_sequence": 2,
            "expected_previous_digest": d1,
        },
    }
    conn.execute(
        "INSERT INTO device_batches "
        "(device_id, sequence, batch_id, previous_digest, digest, observation_count, "
        "thresholds, observations, response, status, created_at) "
        "VALUES (?, 1, 'b-1', '', ?, ?, ?, ?, ?, 'accepted', ?)",
        (
            DEVICE, d1, len(observations1),
            _json.dumps(thresholds1), _json.dumps(observations1),
            _json.dumps(response1), ts1,
        ),
    )
    conn.execute(
        "INSERT INTO device_chains "
        "(device_id, last_sequence, head_digest, batch_count, updated_at) "
        "VALUES (?, 1, ?, 1, ?)",
        (DEVICE, d1, ts1),
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=str(db))
    with TestClient(app) as c:
        # 旧记录重放次数为零、无最近重放时刻；三个查询接口字段含义一致
        cur = c.get(f"/field-devices/{DEVICE}/cursor").json()
        assert cur["last_sequence"] == 1 and cur["batch_count"] == 1
        assert cur["head_digest"] == d1
        # 迁移回填：旧链最近事件为链头批次的受理事件
        assert cur["last_event_status"] == "accepted"
        assert cur["last_event_sequence"] == 1
        assert cur["last_event_at"] is not None

        item = c.get(f"/field-devices/{DEVICE}/batches").json()["batches"][0]
        assert item["status"] == "accepted"
        assert item["replay_count"] == 0
        assert item["last_replayed_at"] is None

        detail = c.get(f"/field-devices/{DEVICE}/batches/1").json()
        assert detail["status"] == "accepted"
        assert detail["replay_count"] == 0
        assert detail["last_replayed_at"] is None
        assert detail["observations"] == observations1
        assert detail["thresholds"] == thresholds1

        # 升级后重放走新逻辑：200 replayed，计数变为 1，游标最近事件更新
        r = upload(c, body1)
        assert r.status_code == 200 and r.json()["status"] == "replayed"
        assert r.json()["digest"] == d1
        detail = c.get(f"/field-devices/{DEVICE}/batches/1").json()
        assert detail["replay_count"] == 1
        assert detail["last_replayed_at"] is not None
        cur = c.get(f"/field-devices/{DEVICE}/cursor").json()
        assert cur["last_event_status"] == "replayed"
        assert cur["last_event_sequence"] == 1
        assert cur["last_sequence"] == 1 and cur["batch_count"] == 1

        # 旧链仍可继续受理新批次，游标最近事件回到 accepted
        r2 = upload(c, batch("b-2", 2, previous_digest=d1))
        assert r2.status_code == 201
        cur = c.get(f"/field-devices/{DEVICE}/cursor").json()
        assert cur["last_sequence"] == 2 and cur["batch_count"] == 2
        assert cur["last_event_status"] == "accepted"
        assert cur["last_event_sequence"] == 2
