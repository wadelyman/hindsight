"""Budget allocator (baseline spec §4.2, M7/G2): water-filling with per-tier
caps and downward spillover, plus Tier-3 dedupe against injected concepts.

Two properties the inline α₁-only version lacked:

- **Spillover.** Unused higher-tier budget flows down, so a bank with no
  concepts gets the full recall budget back for Tier 3 — the difference
  between additive and regressive (with zero concepts, recall must be
  byte-identical to `memories_only`).
- **Dedupe.** A Tier-3 memory already cited in an injected concept's sources
  is dropped — otherwise distillation relocates tokens instead of compressing
  them. This is the mechanism by which OKF raises context density.
"""

from __future__ import annotations

from dataclasses import dataclass, field

DEFAULT_ALPHA = (0.10, 0.30, 0.20, 0.40)  # (α₀, α₁, α₂, α₃) per baseline §4.2


@dataclass
class TierCandidates:
    tier: int
    items: list[dict] = field(default_factory=list)


def _cost(item: dict) -> float:
    return len(item.get("body") or "") / 4


def water_fill(tiers: list[TierCandidates], *, max_tokens: int, alpha: tuple[float, ...] = DEFAULT_ALPHA) -> dict:
    """Greedy allocation, lowest tier first, with downward spillover.

    k_i = min(need_i, α_i·k + spill_{i-1});  spill_i = α_i·k + spill_{i-1} − k_i

    Returns {"tiers": {tier: kept_items}, "spent": {tier: tokens}, "residual": float}.
    Items that don't fit are truncated to the remaining room (marked
    ``truncated: True``); items beyond that are dropped.
    """
    if abs(sum(alpha) - 1.0) > 1e-9:
        raise ValueError("alpha must sum to 1")
    spill = 0.0
    out: dict[int, list[dict]] = {}
    spent: dict[int, float] = {}
    for t in sorted(tiers, key=lambda x: x.tier):
        cap = alpha[t.tier] * max_tokens + spill
        used = 0.0
        kept: list[dict] = []
        for item in t.items:
            cost = _cost(item)
            if used + cost > cap:
                room = int(max(0, (cap - used) * 4))
                if room > 0:
                    kept.append({**item, "body": (item.get("body") or "")[:room], "truncated": True})
                    used = cap
                break
            kept.append(item)
            used += cost
        out[t.tier] = kept
        spent[t.tier] = used
        spill = cap - used
    return {"tiers": out, "spent": spent, "residual": spill}


def dedupe_against_concepts(memories: list[dict], concepts: list[dict]) -> list[dict]:
    """Drop Tier-3 memories already cited by an injected concept's sources
    (baseline §4.2): the same claim must not be paid for twice at two
    compression levels."""
    cited = {
        str(s.get("memory_id"))
        for c in concepts
        for s in (c.get("sources") or [])
        if s.get("memory_id")
    }
    if not cited:
        return memories
    return [m for m in memories if str(m.get("id")) not in cited]
