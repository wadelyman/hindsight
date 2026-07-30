"""Query planner (baseline §4.2, M7/G4): tier selection as a pure, testable
function of (query, mode, config). The same query always yields the same plan,
which is what makes shadow-mode comparisons meaningful.
"""

from __future__ import annotations

import hashlib
import re

TEMPORAL_RE = re.compile(
    r"\b(yesterday|today|last\s+(week|month|night|time)|this\s+(week|month|morning)|before|after|since|until|recent|latest|earliest)\b",
    re.IGNORECASE,
)


def plan(query: str, mode: str, *, okf_enabled: bool = True, semantic_enabled: bool = True) -> dict:
    """Return the execution plan for a recall/semantic request.

    Pure: no I/O. Entity seeds are *candidates* — resolution to actual nodes
    happens at execution; a candidate that resolves to nothing costs one cheap
    lookup (exact-canonical match) and is then dropped.
    """
    q = (query or "").strip()
    tiers: list[int] = []
    if okf_enabled and mode in ("auto", "okf_first", "shadow"):
        tiers.extend([0, 1, 2])  # computation, concept, mental-model — cost-ascending
    tiers.append(3)

    # Entity candidates: short queries that are not sentence-shaped.
    words = q.split()
    looks_entity = bool(q) and len(words) <= 4 and not q.endswith((".", "?", "!"))
    entity_seeds = [q] if looks_entity else []

    return {
        "plan_id": hashlib.sha256(f"{mode}:{q}".encode()).hexdigest()[:12],
        "mode": mode,
        "tiers": tiers,
        "graph_assist": bool(semantic_enabled and looks_entity and mode != "memories_only"),
        "entity_seeds": entity_seeds,
        "temporal_constraint": bool(TEMPORAL_RE.search(q)),
        "tier0_params": None,
    }
