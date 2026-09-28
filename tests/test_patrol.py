"""巡检整体核对：全局分配动态规划与 /patrols/analyze 接口。"""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pytest
from fastapi.testclient import TestClient

from app.main import create_app
from app.patrol import _optimal_assignment

LIBRARY = {
    "aids": [
        {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
        {"aid_id": "B", "period": 6.0, "flashes": [0.0]},
        {"aid_id": "C", "period": 12.0, "flashes": [0.0, 0.8, 5.0, 5.8]},
    ],
    "mode": "merge",
}

PARAMS = {"tolerance": 0.5, "max_drift": 5e-4, "max_missed": 3, "max_false": 3}


@pytest.fixture()
def client(tmp_path):
    app = create_app(db_path=str(tmp_path / "test.db"))
    with TestClient(app) as c:
        yield c


def synthesize(period, flashes, psi, drift, n_cycles, noise=0.0, drop=(), extra=(), seed=1):
    """按观测模型 t_obs = (1+drift)·T + 噪声 合成一个窗口的观测序列。"""
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


# ---------------------------------------------------------------------------
# 全局分配动态规划：裁决顺序与保序约束
# ---------------------------------------------------------------------------

def test_assignment_maximizes_windows_over_per_window_local_best():
    """禁止逐窗局部最佳：窗口 1 的局部最佳在航点 0，全局最优把航点 0 留给窗口 0。"""
    cost = [
        [(0, 0.10, 0.0), None],            # 窗口 0 只能去航点 0
        [(0, 0.05, 0.0), (2, 0.30, 0.0)],  # 窗口 1 局部最佳是航点 0，航点 1 也可行
    ]
    # 逐窗局部最佳会让两个窗口争抢航点 0（只分配 1 个）；全局最优各得其所
    assert _optimal_assignment(cost) == [(0, 0), (1, 1)]


def test_assignment_minimizes_missed_waypoints_in_span():
    """已分配窗口数相同：首末已分配航点间漏巡点更少者胜。"""
    c = (0, 0.10, 0.0)
    cost = [
        [c, None, c],
        [None, c, c],
    ]
    # (0→0, 1→1) 漏巡 0；(0→0, 1→2) 漏巡 1；(0→2, …) 窗口 1 无航点可去
    assert _optimal_assignment(cost) == [(0, 0), (1, 1)]


def test_assignment_tie_breaks_by_error_residual_drift():
    """已分配数与漏巡点相同：依次按漏记+误记合计、残差总和、|钟漂|总和裁决。"""
    # 漏记+误记合计：方案 B（合计 0）优于方案 A（合计 1），尽管 A 的航点序列更小
    cost = [
        [(1, 0.10, 0.0), (0, 0.10, 0.0), None],
        [None, (0, 0.10, 0.0), (0, 0.10, 0.0)],
    ]
    assert _optimal_assignment(cost) == [(0, 1), (1, 2)]
    # 残差总和：合计相同（0），B 的残差总和更小
    cost = [
        [(0, 0.30, 0.0), (0, 0.10, 0.0), None],
        [None, (0, 0.10, 0.0), (0, 0.10, 0.0)],
    ]
    assert _optimal_assignment(cost) == [(0, 1), (1, 2)]
    # |钟漂|总和：合计与残差都相同，B 的 |钟漂| 总和更小
    cost = [
        [(0, 0.10, 0.5), (0, 0.10, 0.1), None],
        [None, (0, 0.10, 0.1), (0, 0.10, 0.1)],
    ]
    assert _optimal_assignment(cost) == [(0, 1), (1, 2)]


def test_assignment_lexicographic_waypoint_sequence():
    """前几项全同：航点索引序列字典序最小者胜。"""
    c = (0, 0.10, 0.0)
    cost = [
        [c, c, None],
        [None, c, c],
    ]
    # (0→0, 1→1) 与 (0→1, 1→2) 各项合计全同 → 航点索引序列 [0, 1] 胜
    assert _optimal_assignment(cost) == [(0, 0), (1, 1)]


def test_assignment_requires_strictly_increasing_waypoints():
    """保序约束：窗口 0 只能去航点 1、窗口 1 只能去航点 0 时二者不可兼得。"""
    c = (0, 0.10, 0.0)
    cost = [
        [None, c],
        [c, None],
    ]
    # 只能分配一个；并列时按裁决顺序取航点索引序列较小者
    assert _optimal_assignment(cost) == [(1, 0)]


def test_assignment_empty_when_no_feasible_pair():
    assert _optimal_assignment([]) == []
    assert _optimal_assignment([[None, None]]) == []


# ---------------------------------------------------------------------------
# /patrols/analyze 接口
# ---------------------------------------------------------------------------

def test_patrol_analyze_end_to_end(client):
    client.post("/library/import", json=LIBRARY)
    version = client.get("/library").json()["library_version"]

    psi = 1_700_000_000.3
    # 航线 WP1(A) → WP2(B) → WP3(C) → WP4(A)；窗口 0 对应 WP1，
    # 窗口 1 是噪声（误记超限，不属于任何航点），窗口 2 对应 WP3
    w0 = synthesize(10.0, [0.0, 1.5], psi, 1e-4, 8, noise=0.02, seed=1)
    w1 = [psi + 900.0 + 0.4 * k for k in range(8)]
    w2 = synthesize(12.0, [0.0, 0.8, 5.0, 5.8], psi + 2000.0, 1e-4, 6, noise=0.02, seed=2)
    r = client.post("/patrols/analyze", json={
        "waypoints": [
            {"waypoint_id": "WP1", "aid_id": "A"},
            {"waypoint_id": "WP2", "aid_id": "B"},
            {"waypoint_id": "WP3", "aid_id": "C"},
            {"waypoint_id": "WP4", "aid_id": "A"},
        ],
        "windows": [
            {"observations": w0},
            {"observations": w1},
            {"observations": w2},
        ],
        **PARAMS,
    })
    assert r.status_code == 200
    body = r.json()
    assert body["status"] == "assigned"
    assert body["reason"] is None
    assert body["library_version"] == version
    assert body["window_count"] == 3 and body["waypoint_count"] == 4
    assert body["parameters"] == PARAMS

    pairs = [(a["window_index"], a["waypoint_index"]) for a in body["assignments"]]
    assert pairs == [(0, 0), (2, 2)]
    assert [a["waypoint_id"] for a in body["assignments"]] == ["WP1", "WP3"]
    # 分配候选证据与 /judge 的 best 结构一致
    cand = body["assignments"][0]["candidate"]
    assert cand["aid_id"] == "A"
    assert cand["matched"] == 16 and cand["missed"] == 0 and cand["false"] == 0
    assert len(cand["events"]) == 16

    # 噪声窗口未分配；WP2 漏巡；WP4 在末个已分配航点之后，不计漏巡
    assert [w["window_index"] for w in body["unassigned_windows"]] == [1]
    assert body["unassigned_windows"][0]["observation_count"] == 8
    assert [m["waypoint_id"] for m in body["missed_waypoints"]] == ["WP2"]

    # 候选证据：窗口 0 的合格候选只有 A，且标注其可用于航点 0 与 3
    ev0 = body["window_candidates"][0]["candidates"]
    assert [c["aid_id"] for c in ev0] == ["A"]
    assert ev0[0]["qualified"] and ev0[0]["waypoint_indices"] == [0, 3]
    assert body["window_candidates"][1]["candidates"] == []
    assert [c["aid_id"] for c in body["window_candidates"][2]["candidates"]] == ["C"]


def test_patrol_analyze_global_assignment_not_per_window_greedy(client):
    """全局 DP 优于逐窗局部最佳：窗口 1 的局部最佳在 WP1，但 WP1 应留给窗口 0。"""
    client.post("/library/import", json={
        "aids": [
            {"aid_id": "A", "period": 10.0, "flashes": [0.0, 1.5]},
            {"aid_id": "B", "period": 10.0, "flashes": [0.0, 1.5, 5.0]},
        ],
    })
    psi = 1_700_000_000.3
    # 窗口 0：10 周期 A，只对 A 合格（对 B 漏记 10 次超限）
    w0 = synthesize(10.0, [0.0, 1.5], psi, 0.0, 10, noise=0.02, seed=3)
    # 窗口 1：5 周期 A，对 A 完美（局部最佳），对 B 也合格（漏记 4 次在上限内，
    # 末个周期的 5.0 闪光在末个匹配之后，按边界规则不计漏记）
    w1 = synthesize(10.0, [0.0, 1.5], psi + 500.0, 0.0, 5, noise=0.02, seed=4)
    body = client.post("/patrols/analyze", json={
        "waypoints": [
            {"waypoint_id": "WP1", "aid_id": "A"},
            {"waypoint_id": "WP2", "aid_id": "B"},
        ],
        "windows": [{"observations": w0}, {"observations": w1}],
        "tolerance": 0.5,
        "max_drift": 1e-4,
        "max_missed": 5,
        "max_false": 3,
    }).json()
    assert body["status"] == "assigned"
    # 逐窗局部最佳会把窗口 1 也抢去 WP1；全局最优：窗口 0→WP1、窗口 1→WP2
    pairs = [(a["window_index"], a["waypoint_index"]) for a in body["assignments"]]
    assert pairs == [(0, 0), (1, 1)]
    assert body["unassigned_windows"] == [] and body["missed_waypoints"] == []
    assert body["assignments"][1]["candidate"]["missed"] == 4


def test_patrol_analyze_no_assignment_reasons(client):
    client.post("/library/import", json=LIBRARY)

    # 无航点
    body = client.post("/patrols/analyze", json={
        "waypoints": [], "windows": [{"observations": [1.0, 2.0]}], **PARAMS,
    }).json()
    assert body["status"] == "no_assignment"
    assert "no waypoints" in body["reason"]
    assert body["assignments"] == [] and body["missed_waypoints"] == []

    # 无观测窗口
    body = client.post("/patrols/analyze", json={
        "waypoints": [{"waypoint_id": "WP1", "aid_id": "A"}], "windows": [], **PARAMS,
    }).json()
    assert body["status"] == "no_assignment"
    assert "no observation windows" in body["reason"]

    # 所有 (窗口, 航点) 对均无合格候选：原因写明排名最高候选的违规
    burst = [100.0 + 0.4 * k for k in range(8)]  # 3 秒内 8 次观测，误记必超限
    body = client.post("/patrols/analyze", json={
        "waypoints": [{"waypoint_id": "WP1", "aid_id": "A"}],
        "windows": [{"observations": burst}],
        **PARAMS,
    }).json()
    assert body["status"] == "no_assignment"
    assert "no qualified window-waypoint pair" in body["reason"]
    assert "WP1" in body["reason"] and "false" in body["reason"]
    assert body["assignments"] == [] and body["missed_waypoints"] == []
    assert body["unassigned_windows"] == [{"window_index": 0, "observation_count": 8}]


def test_patrol_analyze_validation(client):
    client.post("/library/import", json=LIBRARY)

    # 航点 aid_id 不在激活特征库版本中 → 400
    r = client.post("/patrols/analyze", json={
        "waypoints": [{"waypoint_id": "WP1", "aid_id": "NOPE"}],
        "windows": [{"observations": [1.0]}],
        **PARAMS,
    })
    assert r.status_code == 400 and "NOPE" in r.json()["detail"]

    # waypoint_id 重复 → 422
    r = client.post("/patrols/analyze", json={
        "waypoints": [
            {"waypoint_id": "WP1", "aid_id": "A"},
            {"waypoint_id": "WP1", "aid_id": "B"},
        ],
        "windows": [{"observations": [1.0]}],
        **PARAMS,
    })
    assert r.status_code == 422

    # 航点数 > 50 → 422；窗口数 > 20 → 422
    r = client.post("/patrols/analyze", json={
        "waypoints": [{"waypoint_id": f"WP{k}", "aid_id": "A"} for k in range(51)],
        "windows": [],
        **PARAMS,
    })
    assert r.status_code == 422
    r = client.post("/patrols/analyze", json={
        "waypoints": [],
        "windows": [{"observations": []} for _ in range(21)],
        **PARAMS,
    })
    assert r.status_code == 422

    # 窗口观测含非数值类型 → 422（有限性校验与 /judge 一致）
    r = client.post("/patrols/analyze", json={
        "waypoints": [], "windows": [{"observations": ["not-a-number"]}], **PARAMS,
    })
    assert r.status_code == 422


def test_patrol_analyze_unknown_aid_without_active_version(client):
    """无任何激活版本时，航点 aid_id 必然查无 → 400。"""
    r = client.post("/patrols/analyze", json={
        "waypoints": [{"waypoint_id": "WP1", "aid_id": "A"}],
        "windows": [{"observations": [1.0]}],
        **PARAMS,
    })
    assert r.status_code == 400
    assert "no active library version" in r.json()["detail"]


def test_patrol_analyze_pins_active_version(client):
    client.post("/library/import", json=LIBRARY)
    v1 = client.get("/library").json()["library_version"]
    req = {
        "waypoints": [{"waypoint_id": "WP1", "aid_id": "A"}],
        "windows": [{"observations": synthesize(10.0, [0.0, 1.5], 1_700_000_000.3, 0.0, 5, noise=0.02)}],
        **PARAMS,
    }
    body = client.post("/patrols/analyze", json=req).json()
    assert body["status"] == "assigned"
    assert body["library_version"] == v1

    # 激活新版本后：新的核对固定新版本号
    client.post("/library/import", json={
        "aids": [{"aid_id": "D", "period": 8.0, "flashes": [0.0]}],
        "mode": "merge",
    })
    v2 = client.get("/library").json()["library_version"]
    assert v2 != v1
    body = client.post("/patrols/analyze", json=req).json()
    assert body["library_version"] == v2
