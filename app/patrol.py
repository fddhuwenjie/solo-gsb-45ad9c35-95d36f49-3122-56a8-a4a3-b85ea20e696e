"""巡检整体核对：多个观测窗口与计划航线的全局分配。

模型
----
一次巡检沿计划航线依次经过若干航点（顺序固定），途中按采集顺序得到若干
观测窗口。每个窗口最多对应航线上的一个航点，也可能不属于任何航点（可
跳过窗口）；有的航点可能没有任何窗口覆盖（可跳过航点）。窗口到航点的
分配是一对一且保序的：窗口按采集顺序分配到严格递增的航点下标。

算法
----
1. 对每个窗口，用与 /judge 相同的相位-钟漂联合估计
   （``align.estimate_candidate``）逐一评估航线上出现的航标，得到该窗口
   的全部合格航标候选（合格条件与 /judge 一致：漏记、误记、钟漂均不超限）；
2. 用动态规划求全局最优分配（禁止逐窗采用局部最佳）：状态 (i, j) 表示
   最后一次分配为「窗口 i → 航点 j」，转移自任意 i' < i 且 j' < j 的状态，
   或以该对作为首个分配。下列目标均可加性分解、字典序比较在公共扩展下
   保持，故每个状态只保留最优值即可得到全局最优。

裁决顺序（依次比较，前者优先）
------------------------------
1. 已分配窗口最多；
2. 首末已分配航点之间的漏巡点最少（漏巡点 = 首末已分配航点范围内未被
   分配的航点；从航点 j' 扩展到 j 时新增 j - j' - 1 个，可增量累计）；
3. 已分配候选的漏记与误记合计最少；
4. 匹配残差总和最小；
5. 钟漂绝对值总和最小；
6. 航点索引序列字典序最小；
7. （稳定性补充）窗口索引序列字典序最小：前六项完全相同时保证结果
   确定可复现。
"""
from __future__ import annotations

from dataclasses import dataclass

from .align import AidPattern, Candidate, estimate_candidate


@dataclass(frozen=True)
class PatrolWaypoint:
    """计划航线上的一个航点：航点编号 + 该处航标编号。"""

    waypoint_id: str
    aid_id: str


@dataclass
class PatrolAssignment:
    """一次「窗口 → 航点」分配及其候选证据。"""

    window_index: int
    waypoint_index: int
    candidate: Candidate


@dataclass
class PatrolResult:
    """整体核对结果：分配、每个窗口的合格候选（按航标编号）、无分配时的原因。"""

    assignments: list[PatrolAssignment]
    qualified: list[dict[str, Candidate]]
    reason: str | None


def _optimal_assignment(
    cost: list[list[tuple[float, float, float] | None]],
) -> list[tuple[int, int]]:
    """求窗口到航点的全局最优分配，返回按窗口索引升序的 (窗口索引, 航点索引)。

    ``cost[i][j]`` 为 (漏记+误记, 残差总和, |钟漂|)，``None`` 表示窗口 i
    对航点 j 的航标无合格候选、不可分配。
    """
    n_windows = len(cost)
    n_waypoints = len(cost[0]) if n_windows else 0
    # 状态 (i, j)：最后一次分配为「窗口 i → 航点 j」。值元组按裁决顺序排列：
    # (-已分配窗口数, 漏巡点数, 漏记+误记合计, 残差总和, |钟漂|总和,
    #  航点索引序列, 窗口索引序列)
    # 逐项越小越优（首项取负使「已分配最多」排在最前）；序列随分配增长，
    # 最优值中的索引序列即分配方案本身，无需额外回溯。
    dp: dict[tuple[int, int], tuple] = {}
    best: tuple | None = None
    for i in range(n_windows):
        row: dict[tuple[int, int], tuple] = {}
        for j in range(n_waypoints):
            c = cost[i][j]
            if c is None:
                continue
            err, residual, drift = c
            value = (-1, 0, err, residual, drift, (j,), (i,))
            for (_, pj), prev in dp.items():
                if pj >= j:  # 航点下标必须严格递增（i' < i 由逐行更新保证）
                    continue
                extended = (
                    prev[0] - 1,
                    prev[1] + (j - pj - 1),
                    prev[2] + err,
                    prev[3] + residual,
                    prev[4] + drift,
                    prev[5] + (j,),
                    prev[6] + (i,),
                )
                if extended < value:
                    value = extended
            row[(i, j)] = value
            if best is None or value < best:
                best = value
        dp.update(row)
    if best is None:
        return []
    return list(zip(best[6], best[5]))


def analyze_patrol(
    waypoints: list[PatrolWaypoint],
    windows: list[list[float]],
    patterns: dict[str, AidPattern],
    *,
    tolerance: float,
    max_drift: float,
    max_missed: int,
    max_false: int,
    grid_max_steps: int = 201,
    seed_top_k: int = 5,
) -> PatrolResult:
    """整体核对一次巡检：先算每个窗口的合格航标候选，再全局分配到航点。

    ``patterns`` 为请求开始时固定的激活特征库快照（aid_id → 灯质特征）；
    航点的 aid_id 须已存在于其中（由调用方校验）。
    """
    n_windows = len(windows)
    if not waypoints:
        return PatrolResult([], [dict() for _ in windows], "no waypoints provided")
    if n_windows == 0:
        return PatrolResult([], [], "no observation windows provided")

    # 航线上出现的航标去重（保持航线中的首次出现顺序）
    aid_ids = list(dict.fromkeys(wp.aid_id for wp in waypoints))

    # 1. 每个窗口 × 每种航标：与 /judge 相同的联合估计与合格判定
    all_candidates: list[dict[str, Candidate]] = []
    qualified: list[dict[str, Candidate]] = []
    for obs in windows:
        per_aid: dict[str, Candidate] = {}
        per_aid_qualified: dict[str, Candidate] = {}
        for aid_id in aid_ids:
            cand = estimate_candidate(
                patterns[aid_id],
                obs,
                tolerance,
                max_drift,
                max_missed,
                max_false,
                grid_max_steps=grid_max_steps,
                seed_top_k=seed_top_k,
            )
            per_aid[aid_id] = cand
            if cand.qualified:
                per_aid_qualified[aid_id] = cand
        all_candidates.append(per_aid)
        qualified.append(per_aid_qualified)

    # 2. 动态规划全局分配（禁止逐窗局部最佳）
    cost: list[list[tuple[float, float, float] | None]] = []
    for i in range(n_windows):
        row: list[tuple[float, float, float] | None] = []
        for wp in waypoints:
            cand = qualified[i].get(wp.aid_id)
            row.append(
                None
                if cand is None
                else (cand.missed + cand.false, cand.residual_sum, abs(cand.drift))
            )
        cost.append(row)
    pairs = _optimal_assignment(cost)
    if not pairs:
        # 无任何合格 (窗口, 航点) 对：报告裁决键最小候选的违规原因
        top = None
        for i in range(n_windows):
            for j, wp in enumerate(waypoints):
                cand = all_candidates[i][wp.aid_id]
                key = (cand.ranking_key(), i, j)
                if top is None or key < top[0]:
                    top = (key, i, j, cand)
        _, top_i, top_j, top_cand = top
        reason = (
            "no qualified window-waypoint pair; best-ranked pair "
            f"(window {top_i}, waypoint '{waypoints[top_j].waypoint_id}', "
            f"aid '{top_cand.aid_id}') violated: " + "; ".join(top_cand.violations)
        )
        return PatrolResult([], qualified, reason)

    assignments = [
        PatrolAssignment(i, j, qualified[i][waypoints[j].aid_id]) for i, j in pairs
    ]
    return PatrolResult(assignments, qualified, None)
