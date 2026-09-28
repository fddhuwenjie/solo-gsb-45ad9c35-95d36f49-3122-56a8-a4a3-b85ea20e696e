"""核心算法测试：联合估计、循环对齐、周期边界、匹配裁决、候选排序。"""
import os
import random
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from app.align import AidPattern, _match, estimate_candidate, judge


def synthesize(pattern, psi, drift, n_cycles, noise=0.0, drop=(), extra=(), seed=1):
    """按观测模型 t_obs = (1+drift)·T + 噪声 合成观测序列。"""
    rng = random.Random(seed)
    s = 1.0 + drift
    obs = []
    for n in range(n_cycles):
        for j, f in enumerate(pattern.flashes):
            if (n, j) in drop:
                continue
            obs.append(s * (psi + n * pattern.period + f) + rng.uniform(-noise, noise))
    obs.extend(extra)
    obs.sort()
    return obs


A = AidPattern("A", 10.0, (0.0, 1.5))
B = AidPattern("B", 10.0, (0.0, 4.0))
C = AidPattern("C", 6.0, (0.0,))
D = AidPattern("D", 10.0, (0.0, 9.0))  # 闪光紧邻周期边界两侧
E = AidPattern("E", 10.0, (0.0, 0.8))  # 周期内闪光密集，容差窗相互重叠


def test_joint_phase_drift_estimation():
    psi, drift = 1_700_000_000.3, 2e-4
    obs = synthesize(A, psi, drift, 30, noise=0.05)
    cand = estimate_candidate(A, obs, tolerance=0.5, max_drift=5e-4,
                              max_missed=3, max_false=3)
    assert cand.matched == 60
    assert cand.missed == 0
    assert cand.false == 0
    assert abs(cand.drift - drift) < 1e-4
    assert cand.qualified


def test_period_boundary_not_misjudged_as_gap():
    """闪光位于周期边界两侧（间隔 1s），跨周期连续观测不得产生伪漏记。"""
    obs = synthesize(D, 1_700_000_100.0, 0.0, 20, noise=0.02)
    cand = estimate_candidate(D, obs, tolerance=0.5, max_drift=1e-4,
                              max_missed=0, max_false=0)
    assert cand.matched == 40
    assert cand.missed == 0
    assert cand.false == 0
    assert cand.qualified
    cycles = {e.cycle for e in cand.events}
    assert cycles == set(range(20))


def test_missed_and_false_counting():
    psi = 1_700_000_000.3
    drop = {(5, 0), (12, 1)}
    extra = [(1 + 2e-4) * psi + 53.0, (1 + 2e-4) * psi + 57.0]
    obs = synthesize(A, psi, 2e-4, 30, noise=0.05, drop=drop, extra=extra)
    cand = estimate_candidate(A, obs, tolerance=0.5, max_drift=5e-4,
                              max_missed=3, max_false=3)
    assert cand.missed == 2
    assert cand.false == 2
    assert cand.matched == 58
    assert sorted(cand.unmatched_observations) == sorted(extra)
    assert {(m.cycle, m.flash_offset) for m in cand.missed_flashes} == {
        (n, A.flashes[j]) for (n, j) in drop
    }


def test_incomplete_edge_cycles_not_counted_as_missed():
    """观测起止处的不完整周期不计漏记：只保留每周期第 2 个闪光之后的部分。"""
    psi = 1_700_000_000.3
    obs = synthesize(A, psi, 0.0, 10, noise=0.0)
    # 截掉开头 3 个与结尾 3 个观测，模拟起止不完整
    obs = obs[3:-3]
    cand = estimate_candidate(A, obs, tolerance=0.5, max_drift=1e-4,
                              max_missed=0, max_false=0)
    assert cand.missed == 0
    assert cand.false == 0


def test_judge_ranking_and_qualification():
    """正确航标应胜出；扰动候选按裁决键排序。"""
    psi = 1_700_000_000.3
    obs = synthesize(A, psi, 2e-4, 30, noise=0.05)
    best, candidates, reason = judge(
        [C, B, A], obs, tolerance=0.5, max_drift=5e-4, max_missed=3, max_false=3
    )
    assert best is not None and best.aid_id == "A"
    assert reason is None
    keys = [(c.missed + c.false, c.residual_sum, abs(c.drift), c.aid_id) for c in candidates]
    assert keys == sorted(keys)


def test_no_qualified_candidate_reports_reason():
    psi = 1_700_000_000.3
    drop = {(n, 0) for n in range(3, 8)}  # 序列内部漏记 5 次
    obs = synthesize(A, psi, 0.0, 30, noise=0.02, drop=drop)
    best, candidates, reason = judge(
        [A], obs, tolerance=0.5, max_drift=1e-4, max_missed=2, max_false=10
    )
    assert best is None
    assert "missed 5 exceeds limit 2" in reason
    assert candidates[0].violations


def test_empty_inputs():
    best, candidates, reason = judge([], [1.0, 2.0], 0.5, 1e-3, 0, 0)
    assert best is None and "library is empty" in reason
    best, candidates, reason = judge([A], [], 0.5, 1e-3, 0, 0)
    assert best is None and "no observations" in reason


# ---------------------------------------------------------------------------
# 一对一匹配裁决规则：配对数最大 → 残差总和最小 → 配对序列字典序最小
# ---------------------------------------------------------------------------

def test_match_maximizes_pairs_before_residuals():
    """重叠容差窗：先最大化配对数，再最小化残差总和。

    o1=99.6 只能匹配 p0=100.0；o2=100.3 可匹配 p0（残差 0.3）或 p1=100.8
    （残差 0.5）。残差贪心先抢 (o2,p0)，o1 失配只剩 1 对；最优为 2 对。
    """
    assert _match([99.6, 100.3], [100.0, 100.8], 0.5) == [(0, 0), (1, 1)]


def test_match_equal_residual_tie_broken_by_pair_sequence():
    """等残差：总残差相同的方案按 (观测索引, 期望索引) 序列字典序稳定裁决。"""
    # 单观测位于两个期望正中间（残差均 0.5）：取期望索引较小者
    assert _match([100.5], [100.0, 101.0], 0.6) == [(0, 0)]
    # 三种方案配对数均为 2、总残差均 0.4：取配对序列字典序最小者
    assert _match([100.2, 100.6], [100.0, 100.4, 100.8], 0.25) == [(0, 0), (1, 1)]


def _brute_force_match(obs, preds, tolerance):
    """穷举全部一对一匹配，按「配对数 → 负残差和 → 配对序列」取最优（参照实现）。"""
    best_key = None
    best_pairs = []

    def rec(i, used, pairs, count, neg_residual):
        nonlocal best_key, best_pairs
        if i == len(obs):
            key = (count, neg_residual)
            if best_key is None or key > best_key or (key == best_key and pairs < best_pairs):
                best_key = key
                best_pairs = list(pairs)
            return
        rec(i + 1, used, pairs, count, neg_residual)  # 观测 i 不匹配
        for k in range(len(preds)):
            if not used[k] and abs(obs[i] - preds[k]) <= tolerance + 1e-9:
                used[k] = True
                pairs.append((i, k))
                rec(i + 1, used, pairs, count + 1, neg_residual - abs(obs[i] - preds[k]))
                pairs.pop()
                used[k] = False

    rec(0, [False] * len(preds), [], 0, 0.0)
    return best_pairs


def test_match_against_brute_force():
    """与穷举参照实现对比：小规模随机重叠窗下配对序列完全一致。

    时刻取 0.5 的整数倍（浮点可精确表示），残差总和比较无舍入误差。
    """
    rng = random.Random(20260926)
    for _ in range(200):
        n, m = rng.randint(0, 6), rng.randint(0, 6)
        obs = sorted(rng.randrange(0, 25) * 0.5 for _ in range(n))
        preds = sorted(rng.randrange(0, 25) * 0.5 for _ in range(m))
        tol = rng.choice([0.5, 1.0, 1.5, 2.0])
        assert _match(obs, preds, tol) == _brute_force_match(obs, preds, tol)


def test_overlapping_windows_max_pairs_not_greedy_residual():
    """重叠容差窗端到端：残差贪心抢占共享期望闪光会虚增误记并改变合格判定。

    第 3 周期观测改为 o1=psi+29.55（只能匹配 p0=psi+30.0）与 o2=psi+30.3
    （可匹配 p0，残差 0.3；也可匹配 p1=psi+30.8，残差 0.5）。残差贪心先抢
    (o2,p0) 使 o1 成为误记；最优配对 (o1,p0)、(o2,p1) 两对全部匹配。
    """
    psi = 1_700_000_000.0
    obs = synthesize(E, psi, 0.0, 10, noise=0.0)
    obs = [t for t in obs if not (psi + 29.0 < t < psi + 31.5)]  # 替换第 3 周期观测
    obs += [psi + 29.55, psi + 30.3]
    obs.sort()
    cand = estimate_candidate(E, obs, tolerance=0.5, max_drift=1e-4,
                              max_missed=0, max_false=0)
    assert cand.matched == 20
    assert cand.missed == 0
    assert cand.false == 0
    assert cand.qualified
    cycle3 = {e.observed: e for e in cand.events if e.cycle == 3}
    assert cycle3[psi + 29.55].flash_offset == 0.0
    assert cycle3[psi + 30.3].flash_offset == 0.8


def test_dense_cross_period_boundary_matching():
    """密集跨周期边界：容差窗跨周期重叠时不得虚增漏记/误记。

    灯质 D 的相邻闪光跨周期边界仅隔 1s（…+9.0 与下一周期 +0.0）。第 3 周期
    末闪光（psi+39.0）的观测提前到 psi+38.5，第 4 周期首闪光（psi+40.0）的
    观测提前到 psi+39.4：psi+39.4 同时落在 psi+39.0 与 psi+40.0 的容差窗内。
    残差贪心会把 psi+39.4 抢配给 psi+39.0（残差 0.4 < 0.6），使 psi+38.5
    成为误记、psi+40.0 成为伪漏记；最优配对保持逐事件对应。
    """
    psi = 1_700_000_000.0
    obs = synthesize(D, psi, 0.0, 40, noise=0.0)
    obs = [t for t in obs if t not in (psi + 39.0, psi + 40.0)]
    obs += [psi + 38.5, psi + 39.4]
    obs.sort()
    cand = estimate_candidate(D, obs, tolerance=0.6, max_drift=1e-4,
                              max_missed=0, max_false=0)
    assert cand.matched == 80
    assert cand.missed == 0
    assert cand.false == 0
    assert cand.qualified
    by_obs = {e.observed: e for e in cand.events}
    assert (by_obs[psi + 38.5].cycle, by_obs[psi + 38.5].flash_offset) == (3, 9.0)
    assert (by_obs[psi + 39.4].cycle, by_obs[psi + 39.4].flash_offset) == (4, 0.0)
