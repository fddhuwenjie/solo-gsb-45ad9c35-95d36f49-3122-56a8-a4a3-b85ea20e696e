"""HTTP API 测试：特征库导入、判定、判定记录查询。"""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.main import create_app

LIBRARY = {
    "aids": [
        {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "B", "period": 10.0, "flashes": [0.0, 4.0]},
        {"aid_id": "C", "period": 6.0, "flashes": [0.0]},
    ],
    "mode": "merge",
}


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def synthesize(period, flashes, psi, drift, n_cycles, noise=0.0, drop=(), extra=(), seed=1):
    rng = random.Random(seed)
    s = 1.0 + drift
    obs = []
    for n in range(n_cycles):
        for j, f in enumerate(flashes):
            if (n, j) in drop:
                continue
            obs.append(s * (psi + n * period + f) + rng.uniform(-noise, noise))
    obs.extend(extra)
    obs.sort()
    return obs


def make_observations():
    psi, drift = 1_700_000_000.3, 2e-4
    drop = {(5, 0), (12, 1)}
    extra = [(1 + drift) * psi + 53.0, (1 + drift) * psi + 57.0]
    return synthesize(10.0, [0.0, 1.5], psi, drift, 30,
                      noise=0.05, drop=drop, extra=extra)


def test_health(client):
    r = client.get("/health")
    assert r.status_code == 200 and r.json()["status"] == "ok"


def test_import_and_list_library(client):
    r = client.post("/library/import", json=LIBRARY)
    assert r.status_code == 200
    body = r.json()
    assert body["imported"] == 3 and body["total"] == 3

    r = client.get("/library")
    assert r.status_code == 200
    aids = {a["aid_id"]: a for a in r.json()["aids"]}
    assert aids["A"]["flashes"] == [0.0, 1.5]

    # replace 模式：清空后导入
    r = client.post("/library/import", json={
        "aids": [{"aid_id": "Z", "period": 8.0, "flashes": [0.0]}],
        "mode": "replace",
    })
    assert r.status_code == 200
    aids = client.get("/library").json()["aids"]
    assert [a["aid_id"] for a in aids] == ["Z"]


def test_import_validation_error(client):
    r = client.post("/library/import", json={
        "aids": [{"aid_id": "X", "period": 10.0, "flashes": [0.0, 10.0]}],
    })
    assert r.status_code == 422


def test_judge_matched_and_records(client):
    client.post("/library/import", json=LIBRARY)
    obs = make_observations()
    r = client.post("/judge", json={
        "observations": obs,
        "tolerance": 0.5,
        "max_drift": 5e-4,
        "max_missed": 3,
        "max_false": 3,
    })
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "matched"
    assert body["library_source"] == "stored"
    best = body["best"]
    assert best["aid_id"] == "A"
    assert best["missed"] == 2 and best["false"] == 2
    assert best["matched"] == 58
    assert abs(best["drift"] - 2e-4) < 1e-4
    assert len(best["events"]) == 58
    assert len(best["unmatched_observations"]) == 2
    assert len(best["missed_flashes"]) == 2
    assert {c["aid_id"] for c in body["candidates"]} == {"A", "B", "C"}

    # 判定记录可查询
    jid = body["judgment_id"]
    r = client.get("/judgments")
    assert r.status_code == 200
    rows = r.json()["judgments"]
    assert rows and rows[0]["id"] == jid
    assert rows[0]["best_aid_id"] == "A" and rows[0]["status"] == "matched"

    r = client.get(f"/judgments/{jid}")
    assert r.status_code == 200
    detail = r.json()
    assert detail["request"]["tolerance"] == 0.5
    assert detail["response"]["best"]["aid_id"] == "A"

    r = client.get("/judgments/999999")
    assert r.status_code == 404


def test_judge_inline_library_overrides_stored(client):
    client.post("/library/import", json=LIBRARY)
    obs = make_observations()
    r = client.post("/judge", json={
        "observations": obs,
        "tolerance": 0.5,
        "max_drift": 5e-4,
        "max_missed": 3,
        "max_false": 3,
        "library": [{"aid_id": "C", "period": 6.0, "flashes": [0.0]}],
    })
    body = r.json()
    assert body["library_source"] == "inline"
    assert [c["aid_id"] for c in body["candidates"]] == ["C"]


def test_judge_no_candidate_with_reason(client):
    client.post("/library/import", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
    })
    drop = {(n, 0) for n in range(3, 8)}  # 序列内部漏记 5 次
    obs = synthesize(10.0, [0.0, 1.5], 1_700_000_000.3, 0.0, 30, noise=0.02, drop=drop)
    r = client.post("/judge", json={
        "observations": obs,
        "tolerance": 0.5,
        "max_drift": 1e-4,
        "max_missed": 2,
        "max_false": 10,
    })
    body = r.json()
    assert body["status"] == "no_candidate"
    assert body["best"] is None
    assert "missed 5 exceeds limit 2" in body["reason"]
    assert body["candidates"][0]["violations"]


def test_judge_empty_library(client):
    r = client.post("/judge", json={
        "observations": [1.0, 2.0],
        "tolerance": 0.5,
        "max_drift": 1e-3,
        "max_missed": 0,
        "max_false": 0,
    })
    body = r.json()
    assert body["status"] == "no_candidate"
    assert "library is empty" in body["reason"]
    assert body["library_source"] == "stored"
    assert body["library_version"] is None


# ---------------- 特征库版本发布 ----------------

def _active_version(client):
    versions = client.get("/library/versions").json()["versions"]
    return next(v for v in versions if v["status"] == "active")


def test_import_publishes_new_active_version(client):
    body = client.post("/library/import", json=LIBRARY).json()
    v1 = body["version"]
    assert v1["version_id"] == 1
    assert v1["status"] == "active" and v1["source"] == "import"
    assert v1["base_version"] is None and v1["aid_count"] == 3
    assert body["imported"] == 3 and body["total"] == 3

    # merge 导入 → 基于当前版本原子发布新版本，旧版本转 historical
    body = client.post("/library/import", json={
        "aids": [{"aid_id": "D", "period": 8.0, "flashes": [0.0]}],
        "mode": "merge",
    }).json()
    v2 = body["version"]
    assert v2["version_id"] == 2 and v2["status"] == "active"
    assert v2["base_version"] == 1 and v2["aid_count"] == 4
    assert v2["diff"]["added"] == ["D"]

    metas = {v["version_id"]: v for v in client.get("/library/versions").json()["versions"]}
    assert metas[1]["status"] == "historical" and metas[2]["status"] == "active"
    assert client.get("/library").json()["library_version"] == 2


def test_version_lifecycle_diff_and_conflict(client):
    client.post("/library/import", json=LIBRARY)
    v1 = _active_version(client)["version_id"]

    # 创建草稿：新增 D，差异即算即返
    r = client.post("/library/versions", json={
        "aids": LIBRARY["aids"] + [{"aid_id": "D", "period": 8.0, "flashes": [0.0, 2.0]}],
        "base_version": v1,
    })
    assert r.status_code == 201
    d1 = r.json()
    assert d1["status"] == "draft" and d1["base_version"] == v1
    assert d1["diff"] == {"added": ["D"], "removed": [], "modified": []}

    # 差异查询接口与创建响应一致
    r = client.get(f"/library/versions/{d1['version_id']}/diff")
    assert r.json()["base_version"] == v1
    assert r.json()["diff"] == d1["diff"]

    # 并行草稿：只保留 A（删除 B、C）
    r = client.post("/library/versions", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
        "base_version": v1,
    })
    assert r.status_code == 201
    d2 = r.json()
    assert d2["diff"]["removed"] == ["B", "C"]

    # 草稿详情含完整快照
    detail = client.get(f"/library/versions/{d1['version_id']}").json()
    assert [a["aid_id"] for a in detail["aids"]] == ["A", "B", "C", "D"]

    # 激活 d1：v1 转 historical，激活指针原子切换
    r = client.post(f"/library/versions/{d1['version_id']}/activate")
    assert r.status_code == 200 and r.json()["status"] == "active"
    metas = {v["version_id"]: v for v in client.get("/library/versions").json()["versions"]}
    assert metas[v1]["status"] == "historical"
    assert metas[d1["version_id"]]["status"] == "active"
    assert [a["aid_id"] for a in client.get("/library").json()["aids"]] == ["A", "B", "C", "D"]

    # d2 的 base_version 已不再是当前激活版本 → 409，状态保持草稿不变
    r = client.post(f"/library/versions/{d2['version_id']}/activate")
    assert r.status_code == 409
    metas = {v["version_id"]: v for v in client.get("/library/versions").json()["versions"]}
    assert metas[d2["version_id"]]["status"] == "draft"
    assert client.get("/library").json()["library_version"] == d1["version_id"]

    # 以当前激活版本为基线重建内容相同的草稿 → 激活成功
    r = client.post("/library/versions", json={
        "aids": [{"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]}],
        "base_version": d1["version_id"],
    })
    d3 = r.json()
    assert d3["diff"]["removed"] == ["B", "C", "D"]
    assert client.post(f"/library/versions/{d3['version_id']}/activate").status_code == 200
    assert [a["aid_id"] for a in client.get("/library").json()["aids"]] == ["A"]


def test_version_diff_added_removed_modified(client):
    client.post("/library/import", json=LIBRARY)
    v1 = _active_version(client)["version_id"]
    r = client.post("/library/versions", json={
        "aids": [
            {"aid_id": "A", "period": 10.0, "flashes": [0.0, 2.0]},      # modified
            {"aid_id": "B", "period": 10.0, "flashes": [0.0, 4.0]},      # unchanged
            {"aid_id": "D", "period": 6.0, "flashes": [0.0]},            # added
        ],
        "base_version": v1,
    })
    assert r.json()["diff"] == {"added": ["D"], "removed": ["C"], "modified": ["A"]}


def test_version_endpoint_errors(client):
    client.post("/library/import", json=LIBRARY)
    v1 = _active_version(client)["version_id"]

    # 基线不存在 → 404；版本不存在 → 404
    r = client.post("/library/versions", json={"aids": LIBRARY["aids"], "base_version": 999})
    assert r.status_code == 404
    assert client.get("/library/versions/999").status_code == 404
    assert client.get("/library/versions/999/diff").status_code == 404
    assert client.post("/library/versions/999/activate").status_code == 404

    # 激活非草稿（当前激活版本）→ 400
    assert client.post(f"/library/versions/{v1}/activate").status_code == 400

    # 以草稿为基线 → 400
    d = client.post("/library/versions", json={"aids": LIBRARY["aids"], "base_version": v1}).json()
    r = client.post("/library/versions", json={"aids": LIBRARY["aids"], "base_version": d["version_id"]})
    assert r.status_code == 400

    # 重复 aid_id → 422
    r = client.post("/library/versions", json={
        "aids": [
            {"aid_id": "A", "period": 10.0, "flashes": [0.0]},
            {"aid_id": "A", "period": 12.0, "flashes": [1.0]},
        ],
        "base_version": v1,
    })
    assert r.status_code == 422


def test_rollback_via_historical_version(client):
    client.post("/library/import", json=LIBRARY)
    v1 = _active_version(client)["version_id"]

    # 用 replace 发布一个内容完全不同的 v2
    client.post("/library/import", json={
        "aids": [{"aid_id": "Z", "period": 8.0, "flashes": [0.0]}],
        "mode": "replace",
    })
    v2 = _active_version(client)["version_id"]
    assert v2 == v1 + 1
    assert [a["aid_id"] for a in client.get("/library").json()["aids"]] == ["Z"]

    # 历史版本可作为草稿来源；以历史 v1 为基线取回旧快照
    v1_aids = client.get(f"/library/versions/{v1}").json()["aids"]
    r = client.post("/library/versions", json={"aids": v1_aids, "base_version": v1})
    assert r.status_code == 201
    assert r.json()["diff"] == {"added": [], "removed": [], "modified": []}
    d1 = r.json()["version_id"]

    # 基线是历史版本而非当前版本 → 409（提示已过期，需在最新版本上重建）
    assert client.post(f"/library/versions/{d1}/activate").status_code == 409

    # 在当前激活版本 v2 之上重建草稿，差异即为回退内容，激活后完成回退
    r = client.post("/library/versions", json={"aids": v1_aids, "base_version": v2})
    assert r.json()["diff"] == {"added": ["A", "B", "C"], "removed": ["Z"], "modified": []}
    assert client.post(f"/library/versions/{r.json()['version_id']}/activate").status_code == 200
    body = client.get("/library").json()
    assert body["library_version"] == r.json()["version_id"]
    assert [a["aid_id"] for a in body["aids"]] == ["A", "B", "C"]


def test_judge_pins_active_version(client):
    client.post("/library/import", json=LIBRARY)
    v1 = _active_version(client)["version_id"]
    obs = make_observations()

    # 未传内联库：响应与判定记录保存固定下来的 library_version
    body = client.post("/judge", json={
        "observations": obs,
        "tolerance": 0.5,
        "max_drift": 5e-4,
        "max_missed": 3,
        "max_false": 3,
    }).json()
    assert body["library_source"] == "stored"
    assert body["library_version"] == v1

    detail = client.get(f"/judgments/{body['judgment_id']}").json()
    assert detail["library_version"] == v1
    assert detail["response"]["library_version"] == v1
    row = client.get("/judgments").json()["judgments"][0]
    assert row["library_version"] == v1

    # 激活新版本后：新判定固定新版本号，既有判定记录仍指向旧版本（记录不可变）
    client.post("/library/versions", json={
        "aids": LIBRARY["aids"] + [{"aid_id": "D", "period": 10.0, "flashes": [0.0, 1.5]}],
        "base_version": v1,
    })
    draft = client.get("/library/versions").json()["versions"][-1]
    client.post(f"/library/versions/{draft['version_id']}/activate")
    assert _active_version(client)["version_id"] != v1
    body2 = client.post("/judge", json={
        "observations": obs,
        "tolerance": 0.5,
        "max_drift": 5e-4,
        "max_missed": 3,
        "max_false": 3,
    }).json()
    assert body2["library_version"] != v1
    assert client.get(f"/judgments/{body['judgment_id']}").json()["library_version"] == v1

    # 内联库：不绑定服务端版本
    body3 = client.post("/judge", json={
        "observations": obs,
        "tolerance": 0.5,
        "max_drift": 5e-4,
        "max_missed": 3,
        "max_false": 3,
        "library": [{"aid_id": "C", "period": 6.0, "flashes": [0.0]}],
    }).json()
    assert body3["library_source"] == "inline"
    assert body3["library_version"] is None
    detail3 = client.get(f"/judgments/{body3['judgment_id']}").json()
    assert detail3["library_version"] is None


def test_legacy_db_migrated_to_initial_version(tmp_path):
    """首次迁移：旧版平铺 aids 表 → 初始激活版本，judgments 补 library_version 列。"""
    import json as _json
    import sqlite3

    db = tmp_path / "legacy.db"
    conn = sqlite3.connect(db)
    conn.executescript(
        """
        CREATE TABLE aids (
            aid_id   TEXT PRIMARY KEY,
            period   REAL NOT NULL,
            flashes  TEXT NOT NULL
        );
        CREATE TABLE judgments (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            created_at   REAL NOT NULL,
            status       TEXT NOT NULL,
            best_aid_id  TEXT,
            matched      INTEGER NOT NULL DEFAULT 0,
            missed       INTEGER NOT NULL DEFAULT 0,
            false_count  INTEGER NOT NULL DEFAULT 0,
            drift        REAL,
            request      TEXT NOT NULL,
            response     TEXT NOT NULL
        );
        """
    )
    conn.execute(
        "INSERT INTO aids (aid_id, period, flashes) VALUES ('OLD', 10.0, ?)",
        (_json.dumps([0.0, 1.5]),),
    )
    conn.execute(
        "INSERT INTO judgments (created_at, status, request, response) "
        "VALUES (?, 'matched', '{}', '{}')",
        (1_700_000_000.0,),
    )
    conn.commit()
    conn.close()

    app = create_app(db_path=str(db))
    with TestClient(app) as c:
        versions = c.get("/library/versions").json()["versions"]
        assert len(versions) == 1
        v = versions[0]
        assert v["status"] == "active"
        assert v["source"] == "migration"
        assert v["base_version"] is None and v["aid_count"] == 1

        lib = c.get("/library").json()
        assert lib["library_version"] == v["version_id"]
        assert lib["aids"] == [{"aid_id": "OLD", "period": 10.0, "flashes": [0.0, 1.5]}]

        # 历史判定记录可读，迁移前列为 NULL
        j = c.get("/judgments/1").json()
        assert j["status"] == "matched" and j["library_version"] is None

        # 迁移后旧表不存在，新判定固定在迁移出的初始版本上
        raw = sqlite3.connect(db)
        tables = {r[0] for r in raw.execute("SELECT name FROM sqlite_master WHERE type='table'")}
        raw.close()
        assert "aids" not in tables

        body = c.post("/judge", json={
            "observations": [],
            "tolerance": 0.5,
            "max_drift": 5e-4,
            "max_missed": 3,
            "max_false": 3,
        }).json()
        assert body["status"] == "no_candidate"
        assert body["library_version"] == v["version_id"]
