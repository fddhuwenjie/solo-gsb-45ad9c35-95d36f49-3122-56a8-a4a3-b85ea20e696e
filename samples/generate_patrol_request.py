#!/usr/bin/env python3
"""生成一份示例巡检核对请求（3 个航点、3 个观测窗口：1 个噪声窗口、1 个漏巡航点）。

用法：
    python3 samples/generate_patrol_request.py > /tmp/patrol.json
    curl -s -X POST http://localhost:8000/patrols/analyze \
        -H 'Content-Type: application/json' -d @/tmp/patrol.json | python3 -m json.tool
"""
import json
import random
import sys

PSI = 1_700_000_000.3          # 真时域相位（灯质原点）
DRIFT = 2e-4                   # 观测时钟线性钟漂（s/s）


def synthesize(period, flashes, psi, n_cycles, noise=0.05, seed=42):
    """按观测模型 t_obs = (1+drift)·T + 噪声 合成一个窗口的观测序列。"""
    rng = random.Random(seed)
    s = 1.0 + DRIFT
    obs = []
    for n in range(n_cycles):
        for f in flashes:
            obs.append(s * (psi + n * period + f) + rng.uniform(-noise, noise))
    obs.sort()
    return [round(t, 6) for t in obs]


# 航线：WP-01(CN-001) → WP-02(CN-002) → WP-03(CN-003)。
# 窗口 0 对应 WP-01；窗口 1 为噪声（3 秒内 8 次观测，误记必超限，不属于任何航点）；
# 窗口 2 对应 WP-03；WP-02 无窗口覆盖 → 漏巡航点。
windows = [
    synthesize(10.0, [0.0, 1.5], PSI, 8, seed=1),                 # CN-001
    [round(PSI + 900.0 + 0.4 * k, 6) for k in range(8)],          # 噪声窗口
    synthesize(6.0, [0.0], PSI + 2000.0, 10, seed=2),             # CN-003
]

request = {
    "waypoints": [
        {"waypoint_id": "WP-01", "aid_id": "CN-001"},
        {"waypoint_id": "WP-02", "aid_id": "CN-002"},
        {"waypoint_id": "WP-03", "aid_id": "CN-003"},
    ],
    "windows": [{"observations": obs} for obs in windows],
    "tolerance": 0.5,
    "max_drift": 0.0005,
    "max_missed": 3,
    "max_false": 3,
}
json.dump(request, sys.stdout, ensure_ascii=False, indent=2)
sys.stdout.write("\n")
