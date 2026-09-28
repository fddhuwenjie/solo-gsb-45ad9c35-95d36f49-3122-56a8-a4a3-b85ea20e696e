#!/usr/bin/env python3
"""生成一份示例判定请求（合成观测：含线性钟漂、噪声、2 次漏记、2 次误记）。

用法：
    python3 samples/generate_request.py > /tmp/judge.json
    curl -s -X POST http://localhost:8000/judge \
        -H 'Content-Type: application/json' -d @/tmp/judge.json | python3 -m json.tool
"""
import json
import random
import sys

PSI = 1_700_000_000.3          # 真时域相位（灯质原点）
PERIOD = 10.0
FLASHES = [0.0, 1.5]           # 对应 samples/library.json 中的 CN-001
DRIFT = 2e-4                   # 观测时钟线性钟漂（s/s）
DROP = {(5, 0), (12, 1)}       # 漏记：(周期序号, 周期内闪光序号)
EXTRA_OFFSETS = [53.0, 57.0]   # 误记：相对首个观测的偏移秒数

rng = random.Random(42)
s = 1.0 + DRIFT
obs = []
for n in range(30):
    for j, f in enumerate(FLASHES):
        if (n, j) in DROP:
            continue
        obs.append(s * (PSI + n * PERIOD + f) + rng.uniform(-0.05, 0.05))
obs += [s * PSI + off for off in EXTRA_OFFSETS]
obs.sort()

request = {
    "observations": [round(t, 6) for t in obs],
    "tolerance": 0.5,
    "max_drift": 0.0005,
    "max_missed": 3,
    "max_false": 3,
}
json.dump(request, sys.stdout, ensure_ascii=False, indent=2)
sys.stdout.write("\n")
