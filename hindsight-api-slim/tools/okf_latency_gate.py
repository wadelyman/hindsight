"""M4 formal gate: recall p99 with OKF resolve on vs off must be within +5% (spec §7).

Runs N recalls per mode against a live daemon and compares latency
distributions. The Tier-3 pipeline is identical in both modes; `auto` adds the
Tier-1 exact lookup, which must be invisible at p99.

Usage: python okf_latency_gate.py [n]
Exit 0 = gate passed, 1 = failed.
"""

import json
import statistics
import sys
import time
import urllib.request

BASE = "http://127.0.0.1:8888/v1/default/banks/okf-test/memories/recall"
QUERIES = [
    "Project Zephyr",
    "Maria Chen",
    "latency",
    "Wade preferences",
    "egress proxy incidents",
    "Halcyon protocol",
    "zbench",
    "Meridian gateway decision",
]

N = int(sys.argv[1]) if len(sys.argv) > 1 else 60
WARMUP = 6


def timed_recall(query: str, resolve: str) -> float:
    body = json.dumps({"query": query, "budget": "mid", "max_tokens": 2000, "resolve": resolve}).encode()
    req = urllib.request.Request(BASE, data=body, headers={"Content-Type": "application/json"})
    t0 = time.perf_counter()
    with urllib.request.urlopen(req, timeout=60) as resp:
        resp.read()
    return time.perf_counter() - t0


def run(mode: str, n: int) -> list[float]:
    # Warmup
    for i in range(WARMUP):
        timed_recall(QUERIES[i % len(QUERIES)], mode)
    return [timed_recall(QUERIES[i % len(QUERIES)], mode) for i in range(n)]


def pct(values: list[float], p: float) -> float:
    s = sorted(values)
    idx = min(len(s) - 1, max(0, round((p / 100) * len(s)) - 1))
    return s[idx]


def main() -> int:
    print(f"latency gate: {N} recalls/mode (warmup {WARMUP})")
    off = run("memories_only", N)
    on = run("auto", N)
    rows = []
    for label, values in (("off", off), ("on", on)):
        rows.append(
            (
                label,
                statistics.median(values),
                pct(values, 95),
                pct(values, 99),
                statistics.mean(values),
            )
        )
    for label, p50, p95, p99, mean in rows:
        print(f"  {label:>4}: p50={p50*1000:7.1f}ms  p95={p95*1000:7.1f}ms  p99={p99*1000:7.1f}ms  mean={mean*1000:7.1f}ms")
    off_p99, on_p99 = rows[0][3], rows[1][3]
    ratio = on_p99 / off_p99 if off_p99 else float("inf")
    print(f"  p99 ratio (on/off): {ratio:.3f}  (gate: <= 1.05)")
    ok = ratio <= 1.05
    print("GATE:", "PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
