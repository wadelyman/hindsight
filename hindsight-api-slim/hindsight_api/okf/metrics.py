"""OKF metrics (M7/G4 tier-hit counter; M8/G7 groundwork).

``semantic_tier_hit_total{tier, mode, bank}`` — incremented once per tier that
contributed ≥1 item to a recall response. Emitting for tiers that ran but
contributed nothing would make the headline value metric meaningless.
Falls back to a structured log line when no OTel meter is configured.
"""

from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

_counter = None


def _get_counter():
    global _counter
    if _counter is None:
        try:
            from ..metrics import get_meter

            _counter = get_meter().create_counter(
                "semantic_tier_hit_total",
                description="Recalls where the tier contributed at least one item, by tier/mode/bank",
            )
        except Exception:
            _counter = False
    return _counter or None


def record_tier_hit(*, tier: int, mode: str, bank: str) -> None:
    attrs = {"tier": str(tier), "mode": mode, "bank": bank}
    counter = _get_counter()
    if counter is not None:
        try:
            counter.add(1, attrs)
            return
        except Exception:
            pass
    logger.info("semantic_tier_hit_total +1", extra={"semantic_tier_hit": attrs})
