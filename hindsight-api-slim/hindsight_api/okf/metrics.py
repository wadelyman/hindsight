"""OKF metrics (§6.11, M7/G4 tier-hit + M8/G7 full set).

Emission model: counters/histograms via the OTel meter when configured
(falling back to structured logs); state gauges (queue depth, suspects,
ratios, eligibility, slot utilisation) via a small module registry read by
observable-gauge callbacks. A tier/metric is only reported when it actually
happened or contributed — silent tiers stay invisible by design.
"""

from __future__ import annotations

import logging
import time

logger = logging.getLogger(__name__)

# name -> ("counter"|"histogram", description)
INSTRUMENTS: dict[str, tuple[str, str]] = {
    "semantic_tier_hit_total": ("counter", "Recalls where the tier contributed at least one item, by tier/mode/bank"),
    "okf_distill_duration_seconds": ("histogram", "okf_distill job duration by class"),
    "okf_dirty_age_seconds": ("histogram", "Age of claimed okf_dirty rows at claim time"),
    "okf_content_hash_skip_total": ("counter", "Projection upserts skipped because content_hash was unchanged"),
    "semantic_query_hops": ("histogram", "Hops executed per semantic traversal"),
    "semantic_nodes_examined": ("histogram", "Edges examined per semantic traversal"),
    "semantic_truncated_total": ("counter", "Traversals truncated by node budget or hop cap"),
    "okf_recursive_dirty_total": ("counter", "W9 violations (must remain 0)"),
    "okf_bundle_export_total": ("counter", "Bundle exports by actor/redaction policy"),
    "okf_recall_total": ("counter", "Recalls by bank/mode (denominator for injection ratio)"),
}

GAUGES: dict[str, str] = {
    "okf_dirty_depth": "Rows currently in okf_dirty, by bank",
    "okf_coalescing_ratio": "Dirty marks attempted per resulting row (≫1 expected; ≈1 means coalescing is broken)",
    "okf_synthesis_eligibility_rate": "Share of stable projection concepts eligible for synthesis (alert > 0.15)",
    "okf_read_write_ratio": "reads per source_generation, by concept path (r̂ governor)",
    "okf_injection_ratio_epsilon": "Tier-1 hit rate over recent recalls, by bank",
    "worker_slot_utilization": "Active okf_* ops vs reservation, by type",
    "okf_i4_suspect_total": "Concepts currently flagged I4-suspect, by bank (should page when nonzero)",
    "okf_recursive_dirty_gauge": "Recursive dirty violations observed in-process (must remain 0)",
}

_instruments: dict = {}
_gauge_instruments_registered = False
_STATE: dict[tuple[str, tuple], float] = {}


def _meter():
    try:
        from ..metrics import get_meter

        return get_meter()
    except Exception:
        return None


def _ensure_instruments() -> None:
    meter = _meter()
    if meter is None or _instruments:
        return
    for name, (kind, desc) in INSTRUMENTS.items():
        try:
            _instruments[name] = meter.create_counter(name, description=desc) if kind == "counter" else meter.create_histogram(name, description=desc)
        except Exception:
            _instruments[name] = None


def _ensure_gauges() -> None:
    global _gauge_instruments_registered
    if _gauge_instruments_registered:
        return
    meter = _meter()
    if meter is None:
        return
    _ensure_instruments()
    for name, desc in GAUGES.items():
        def _callback_factory(metric_name: str):
            def _callback(options):
                from opentelemetry.metrics import Observation

                return [
                    Observation(value, dict(attrs))
                    for (m, attrs), value in _STATE.items()
                    if m == metric_name
                ]

            return _callback

        try:
            meter.create_observable_gauge(name, callbacks=[_callback_factory(name)], description=desc)
        except Exception as e:
            logger.warning(f"okf metric: failed to register gauge {name}: {type(e).__name__}: {e}")
    _gauge_instruments_registered = True


def inc(name: str, n: int = 1, **attrs) -> None:
    _ensure_instruments()
    instrument = _instruments.get(name)
    if instrument is not None:
        try:
            instrument.add(n, attrs)
        except Exception:
            pass


def observe(name: str, value: float, **attrs) -> None:
    _ensure_instruments()
    instrument = _instruments.get(name)
    if instrument is not None:
        try:
            instrument.record(value, attrs)
        except Exception:
            pass


def gauge(name: str, value: float, **attrs) -> None:
    _ensure_gauges()
    _STATE[(name, tuple(sorted(attrs.items())))] = value


def record_tier_hit(*, tier: int, mode: str, bank: str) -> None:
    inc("semantic_tier_hit_total", 1, tier=str(tier), mode=mode, bank=bank)


class Timer:
    """Duration histogram helper: `with Timer("okf_distill_duration_seconds", class_="projection"):`."""

    def __init__(self, metric: str, **attrs):
        self.metric = metric
        self.attrs = attrs
        self.t0 = 0.0

    def __enter__(self):
        self.t0 = time.time()
        return self

    def __exit__(self, *exc):
        observe(self.metric, time.time() - self.t0, **self.attrs)
        return False
