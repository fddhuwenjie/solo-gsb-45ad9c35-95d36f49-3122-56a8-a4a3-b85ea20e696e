"""相位-钟漂联合估计与跨周期循环对齐核心算法。

观测模型
--------
观测时钟相对真时存在线性钟漂与固定相位差：

    t_obs ≈ s · T_true + c,  s = 1 + drift

航标在真时域的闪光时刻为 ``T = ψ + n·P + f_j``（ψ 为相位/灯质原点，
P 为周期，f_j 为周期内第 j 个闪光时刻，n 为周期序号）。合并常数项后：

    t_obs ≈ s · (ψ + n·P + f_j)

估计流程
--------
1. 在 [-max_drift, +max_drift] 内自适应网格搜索钟漂；
2. 每个钟漂下，用 ``(t/s - f_j) mod P`` 循环投票估计相位 ψ
   （滑动窗口 + 圆周均值，天然处理周期边界回绕）；
3. 取投票数最高的若干种子，交替执行「循环匹配 → 约束最小二乘」细化；
4. 期望闪光序列按周期连续生成，跨周期边界不会产生伪缺口；
   漏记只在首个与末个匹配闪光之间统计，观测起止不完整周期不计漏记。

观测与期望的一对一匹配（细化迭代与最终候选构建使用同一规则，见
``_match``）：先最大化配对数，再最小化绝对残差总和，仍相同则按配对的
(观测索引, 期望索引) 序列字典序裁决。重叠容差窗下残差贪心会抢占共享
期望闪光、压低配对数，从而虚增漏记/误记，故不用贪心。
"""
from __future__ import annotations

import bisect
import math
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# 数据结构
# ---------------------------------------------------------------------------

@dataclass(frozen=True)
class AidPattern:
    """航标灯质特征：编号、周期、周期内闪光时刻（秒，[0, period)）。"""

    aid_id: str
    period: float
    flashes: tuple[float, ...]


@dataclass
class FlashEvent:
    """一条匹配上的观测-期望对应关系。"""

    observed: float            # 观测时间戳
    expected_true_time: float  # 期望闪光的真时时刻
    predicted_observed: float  # 按模型预测的观测时刻
    cycle: int                 # 周期序号
    flash_offset: float        # 周期内闪光时刻
    residual: float            # observed - predicted_observed

    def to_dict(self) -> dict:
        return {
            "observed": self.observed,
            "expected_true_time": self.expected_true_time,
            "predicted_observed": self.predicted_observed,
            "cycle": self.cycle,
            "flash_offset": self.flash_offset,
            "residual": self.residual,
        }


@dataclass
class MissedFlash:
    """一次漏记：期望闪光落在观测窗口内（首末匹配之间）却无对应观测。"""

    expected_true_time: float
    cycle: int
    flash_offset: float

    def to_dict(self) -> dict:
        return {
            "expected_true_time": self.expected_true_time,
            "cycle": self.cycle,
            "flash_offset": self.flash_offset,
        }


@dataclass
class Candidate:
    """单个航标候选的对齐结果。"""

    aid_id: str
    offset: float = 0.0                       # 相位 ψ（真时域，mod period）
    drift: float = 0.0                        # 线性钟漂 d = s - 1
    matched: int = 0
    missed: int = 0
    false: int = 0
    residual_sum: float = 0.0
    qualified: bool = False
    violations: list[str] = field(default_factory=list)
    events: list[FlashEvent] = field(default_factory=list)
    missed_flashes: list[MissedFlash] = field(default_factory=list)
    unmatched_observations: list[float] = field(default_factory=list)

    def ranking_key(self) -> tuple:
        """裁决顺序：漏记+误记总数 → 残差总和 → |钟漂| → 航标编号。"""
        return (self.missed + self.false, self.residual_sum, abs(self.drift), self.aid_id)

    def to_summary(self) -> dict:
        return {
            "aid_id": self.aid_id,
            "qualified": self.qualified,
            "violations": self.violations,
            "offset": self.offset,
            "drift": self.drift,
            "matched": self.matched,
            "missed": self.missed,
            "false": self.false,
            "residual_sum": self.residual_sum,
        }

    def to_detail(self) -> dict:
        detail = self.to_summary()
        detail.update(
            {
                "events": [e.to_dict() for e in self.events],
                "missed_flashes": [m.to_dict() for m in self.missed_flashes],
                "unmatched_observations": self.unmatched_observations,
            }
        )
        return detail


# ---------------------------------------------------------------------------
# 相位投票
# ---------------------------------------------------------------------------

def _circular_mean(values: list[float], period: float) -> float:
    """圆周均值，避免 0 与 period 边界处直接算术平均出错。"""
    sx = sum(math.cos(2.0 * math.pi * v / period) for v in values)
    sy = sum(math.sin(2.0 * math.pi * v / period) for v in values)
    return (math.atan2(sy, sx) / (2.0 * math.pi) * period) % period


def _vote_offset(obs: list[float], pattern: AidPattern, drift: float, tolerance: float) -> tuple[float, int]:
    """给定钟漂，对相位做循环投票，返回 (相位估计, 窗口内票数)。

    对每个观测 t 与每个周期内闪光 f，候选相位为 ((t/s - f) mod P)；
    正确相位附近会聚集大量投票。排序后复制一倍做滑动窗口，
    周期边界（0 与 P 相接）处的聚集不会被切断。
    """
    s = 1.0 + drift
    p = pattern.period
    votes: list[float] = []
    for t in obs:
        u = (t / s) % p
        for f in pattern.flashes:
            votes.append((u - f) % p)
    if not votes:
        return 0.0, 0
    votes.sort()
    width = min(2.0 * tolerance, p)
    doubled = votes + [v + p for v in votes]
    best_count = 0
    best_members: list[float] = []
    j = 0
    n = len(votes)
    for i in range(n):
        if j < i:
            j = i
        while j < len(doubled) and doubled[j] - doubled[i] <= width:
            j += 1
        if j - i > best_count:
            best_count = j - i
            best_members = doubled[i:j]
    offset = _circular_mean([v % p for v in best_members], p)
    return offset, best_count


# ---------------------------------------------------------------------------
# 期望闪光生成与匹配
# ---------------------------------------------------------------------------

def _expected_flashes(
    pattern: AidPattern, psi: float, s: float, t_lo: float, t_hi: float
) -> list[tuple[float, float, int, float]]:
    """生成预测观测时刻落在 [t_lo, t_hi] 内的全部期望闪光。

    返回 (predicted_observed, expected_true_time, cycle, flash_offset)，
    按预测时刻升序。周期逐个连续生成，跨周期边界不产生伪缺口。
    """
    p = pattern.period
    T_lo, T_hi = t_lo / s, t_hi / s
    f_min, f_max = min(pattern.flashes), max(pattern.flashes)
    n0 = math.floor((T_lo - psi - f_max) / p)
    n1 = math.ceil((T_hi - psi - f_min) / p)
    eps = 1e-9 * max(1.0, abs(t_lo), abs(t_hi))
    out: list[tuple[float, float, int, float]] = []
    for n in range(n0, n1 + 1):
        for f in pattern.flashes:
            T = psi + n * p + f
            pred = s * T
            if t_lo - eps <= pred <= t_hi + eps:
                out.append((pred, T, n, f))
    out.sort(key=lambda x: x[0])
    return out


def _match(obs: list[float], preds: list[float], tolerance: float) -> list[tuple[int, int]]:
    """观测与预测时刻的一对一匹配，返回按观测索引升序的 (obs_idx, pred_idx) 对。

    三级裁决规则（依次比较，前者优先）：

    1. 配对数最大；
    2. 配对数相同时，配对的绝对残差总和最小；
    3. 总残差仍相同时，配对的 (观测索引, 期望索引) 序列字典序最小（稳定裁决）。

    重叠容差窗下残差贪心会抢占共享期望闪光、压低配对数（虚增漏记/误记），
    故改用动态规划求最优匹配。观测与预测均按时间升序、代价为绝对残差
    （满足 Monge 性质），最优匹配必可取不交叉形式：i < i' 而 k' < k 交叉时，
    交换为 (i, k')、(i', k) 两边仍在容差窗内且总残差不增。令 F(i, k) 为
    obs[i:] 与 preds[k:] 的最优 (配对数, 负残差和)，按后缀递推：

        F(i, k) = max( F(i+1, k),            # 跳过观测 i
                       F(i, k+1),            # 跳过预测 k
                       (1, -|obs[i]-preds[k]|) + F(i+1, k+1) )   # 配对

    每个观测的容差窗只覆盖预测的一段连续区间 [L_i, R_i]，DP 仅在该带状
    区间求值，时间与内存与容差窗内候选边总数同阶。重建时逐对选取保持
    最优值的最小 (i, k)，得到字典序最小的配对序列。
    """
    n, m = len(obs), len(preds)
    if n == 0 or m == 0:
        return []
    tol = tolerance + 1e-9
    # 每个观测在容差窗内可匹配的预测下标区间 [L, R]（随观测升序单调不移）
    L = [0] * n
    R = [-1] * n
    for i, t in enumerate(obs):
        L[i] = bisect.bisect_left(preds, t - tol)
        R[i] = bisect.bisect_right(preds, t + tol) - 1
    # nxt[i]：下标 >= i 且候选区间非空的首个观测；空区间观测的 F 值与后继相同
    nxt: list[int | None] = [None] * (n + 1)
    for i in range(n - 1, -1, -1):
        nxt[i] = i if L[i] <= R[i] else nxt[i + 1]

    ZERO = (0, 0.0)
    # rows[i]：观测 i 的 DP 行，列 k ∈ [L[i], R[i] + 1]；候选区间为空的观测为 None
    rows: list[list[tuple[int, float]] | None] = [None] * n

    def cell(i: int, k: int) -> tuple[int, float]:
        """F(i, k)：obs[i:] 与 preds[k:] 的最优 (配对数, 负残差和)。"""
        while i < n:
            j = nxt[i]
            if j is None:
                return ZERO
            if k < L[j]:
                k = L[j]  # preds[k:L[j]] 对 obs[j:] 无用
            if k <= R[j] + 1:
                return rows[j][k - L[j]]
            i = j + 1  # preds[k:] 对 obs[j] 无用，等价于跳过该观测
        return ZERO

    for i in range(n - 1, -1, -1):
        if L[i] > R[i]:
            continue
        lo, hi = L[i], R[i]
        row = [ZERO] * (hi - lo + 2)
        # 边界列 k = hi + 1：窗内预测已尽，只能跳过观测 i
        row[hi + 1 - lo] = cell(i + 1, hi + 1)
        for k in range(hi, lo - 1, -1):
            best = cell(i + 1, k)            # 跳过观测 i
            skip_pred = row[k + 1 - lo]      # 跳过预测 k
            if skip_pred > best:
                best = skip_pred
            nxt_cell = cell(i + 1, k + 1)    # 配对 (i, k)
            matched = (nxt_cell[0] + 1, nxt_cell[1] - abs(obs[i] - preds[k]))
            if matched > best:
                best = matched
            row[k - lo] = best
        rows[i] = row

    # 重建：在保持最优 (配对数, 负残差和) 的方案中，逐对取最小的 (观测索引, 期望索引)
    pairs: list[tuple[int, int]] = []
    value = cell(0, 0)
    i0, k0 = 0, 0
    while value[0] > 0:
        found = False
        for i in range(i0, n):
            if L[i] > R[i]:
                continue
            k = max(k0, L[i])
            while k <= R[i]:
                suffix = cell(i + 1, k + 1)
                if (suffix[0] + 1, suffix[1] - abs(obs[i] - preds[k])) == value:
                    pairs.append((i, k))
                    value = suffix
                    i0, k0 = i + 1, k + 1
                    found = True
                    break
                k += 1
            if found:
                break
        if not found:
            break  # 防御：最优值非零时必存在可行配对，不会走到这里
    return pairs


def _fit(bases: list[float], ts: list[float], max_drift: float, s_fallback: float) -> tuple[float, float]:
    """约束最小二乘拟合 t = s·B + beta，s 限制在 [1-max_drift, 1+max_drift]。"""
    n = len(bases)
    mean_b = sum(bases) / n
    mean_t = sum(ts) / n
    var_b = sum((b - mean_b) ** 2 for b in bases)
    if var_b < 1e-9:
        s = s_fallback  # 期望时刻退化（如单周期单闪光），无法辨识钟漂
    else:
        cov = sum((bases[i] - mean_b) * (ts[i] - mean_t) for i in range(n))
        s = cov / var_b
        s = min(max(s, 1.0 - max_drift), 1.0 + max_drift)
    beta = mean_t - s * mean_b
    return s, beta


def _refine(
    pattern: AidPattern,
    obs: list[float],
    s: float,
    psi: float,
    tolerance: float,
    max_drift: float,
    max_iter: int = 12,
) -> tuple[float, float]:
    """交替执行循环匹配与约束最小二乘，细化 (s, ψ)。"""
    beta = s * psi
    for _ in range(max_iter):
        cur_psi = beta / s
        flashes = _expected_flashes(pattern, cur_psi, s, obs[0] - tolerance, obs[-1] + tolerance)
        preds = [f[0] for f in flashes]
        pairs = _match(obs, preds, tolerance)
        if len(pairs) < 2:
            break
        bases = [flashes[k][2] * pattern.period + flashes[k][3] for (_, k) in pairs]
        ts = [obs[i] for (i, _) in pairs]
        new_s, new_beta = _fit(bases, ts, max_drift, s)
        delta = abs(new_s - s) + abs(new_beta - beta)
        s, beta = new_s, new_beta
        if delta < 1e-9:
            break
    return s, beta / s


def _normalize_phase(psi: float, s: float, period: float, obs: list[float]) -> float:
    """将相位平移整数个周期，使首个观测附近的期望闪光周期序号为 0。

    ψ 在 mod period 意义下等价；归一化让事件中的周期序号从 0 起排，便于阅读。
    """
    n_shift = round((obs[0] / s - psi) / period)
    return psi + n_shift * period


# ---------------------------------------------------------------------------
# 候选评估
# ---------------------------------------------------------------------------

def _build_candidate(
    pattern: AidPattern,
    obs: list[float],
    s: float,
    psi: float,
    tolerance: float,
    max_drift: float,
    max_missed: int,
    max_false: int,
) -> Candidate:
    flashes = _expected_flashes(pattern, psi, s, obs[0] - tolerance, obs[-1] + tolerance)
    preds = [f[0] for f in flashes]
    pairs = _match(obs, preds, tolerance)
    matched_o = {i for i, _ in pairs}
    matched_k = {k for _, k in pairs}

    events = [
        FlashEvent(
            observed=obs[i],
            expected_true_time=flashes[k][1],
            predicted_observed=flashes[k][0],
            cycle=flashes[k][2],
            flash_offset=flashes[k][3],
            residual=obs[i] - flashes[k][0],
        )
        for (i, k) in pairs
    ]
    # 漏记：仅统计首个与末个匹配闪光之间的期望闪光，
    # 观测起止处的不完整周期不计入，周期边界不会被误判为缺口。
    missed_flashes: list[MissedFlash] = []
    if pairs:
        ks = [k for _, k in pairs]
        for k in range(min(ks), max(ks) + 1):
            if k not in matched_k:
                missed_flashes.append(
                    MissedFlash(
                        expected_true_time=flashes[k][1],
                        cycle=flashes[k][2],
                        flash_offset=flashes[k][3],
                    )
                )
    unmatched = [obs[i] for i in range(len(obs)) if i not in matched_o]

    cand = Candidate(
        aid_id=pattern.aid_id,
        offset=psi % pattern.period,
        drift=s - 1.0,
        matched=len(events),
        missed=len(missed_flashes),
        false=len(unmatched),
        residual_sum=sum(abs(e.residual) for e in events),
        events=events,
        missed_flashes=missed_flashes,
        unmatched_observations=unmatched,
    )
    if cand.missed > max_missed:
        cand.violations.append(f"missed {cand.missed} exceeds limit {max_missed}")
    if cand.false > max_false:
        cand.violations.append(f"false {cand.false} exceeds limit {max_false}")
    if abs(cand.drift) > max_drift + 1e-9:
        cand.violations.append(f"drift {cand.drift:+.3e} exceeds limit ±{max_drift:.3e}")
    cand.qualified = not cand.violations
    return cand


def estimate_candidate(
    pattern: AidPattern,
    observations: list[float],
    tolerance: float,
    max_drift: float,
    max_missed: int,
    max_false: int,
    grid_max_steps: int = 201,
    seed_top_k: int = 5,
) -> Candidate:
    """对单个航标候选做相位-钟漂联合估计与循环对齐。"""
    obs = sorted(float(t) for t in observations)
    if not obs:
        cand = Candidate(aid_id=pattern.aid_id)
        cand.violations.append("no observations provided")
        return cand

    span = obs[-1] - obs[0]
    if max_drift <= 0.0 or span <= 0.0:
        drifts = [0.0]
    else:
        # 自适应步长：一个步长在整个观测跨度上造成的残差变化不超过容差的 1/4
        step = tolerance / (4.0 * span)
        n = max(1, min(int(math.ceil(2.0 * max_drift / step)), grid_max_steps))
        drifts = {-max_drift + 2.0 * max_drift * i / n for i in range(n + 1)}
        drifts.add(0.0)
        drifts = sorted(drifts)

    # 投票阶段对观测抽样（保持跨度），控制大规模观测的计算量
    if len(obs) > 300:
        stride = max(1, len(obs) // 300)
        sample = obs[::stride]
        if sample[-1] != obs[-1]:
            sample.append(obs[-1])
    else:
        sample = obs

    seeds = []
    for d in drifts:
        offset, count = _vote_offset(sample, pattern, d, tolerance)
        seeds.append((-count, abs(d), d, offset))
    seeds.sort()

    best: Candidate | None = None
    for _, _, d, offset in seeds[:seed_top_k]:
        s, psi = _refine(pattern, obs, 1.0 + d, offset, tolerance, max_drift)
        psi = _normalize_phase(psi, s, pattern.period, obs)
        cand = _build_candidate(pattern, obs, s, psi, tolerance, max_drift, max_missed, max_false)
        if best is None or cand.ranking_key() < best.ranking_key():
            best = cand
    return best


def judge(
    patterns: list[AidPattern],
    observations: list[float],
    tolerance: float,
    max_drift: float,
    max_missed: int,
    max_false: int,
    grid_max_steps: int = 201,
    seed_top_k: int = 5,
) -> tuple[Candidate | None, list[Candidate], str | None]:
    """评估全部候选并裁决。

    返回 (最佳合格候选, 全部候选（按裁决顺序升序）, 无合格候选时的原因)。
    """
    candidates = [
        estimate_candidate(
            p, observations, tolerance, max_drift, max_missed, max_false,
            grid_max_steps=grid_max_steps, seed_top_k=seed_top_k,
        )
        for p in patterns
    ]
    candidates.sort(key=lambda c: c.ranking_key())
    qualified = [c for c in candidates if c.qualified]
    best = qualified[0] if qualified else None

    reason = None
    if best is None:
        if not patterns:
            reason = "feature library is empty: import aids via POST /library/import or pass an inline library"
        elif not observations:
            reason = "no observations provided"
        else:
            top = candidates[0]
            reason = (
                f"no qualified candidate; best-ranked aid '{top.aid_id}' violated: "
                + "; ".join(top.violations)
            )
    return best, candidates, reason
