"""W10 hub-skew throttle (baseline §6.3): per-subject minimum refresh interval
with a hub-degree multiplier.

Without it, one high-degree entity — "the user", in every real conversational
bank — is dirtied by nearly every retain and consumes the whole distillation
budget, starving every other concept. A subject last projected less than
``interval × hub_multiplier`` ago is deferred (left dirty for a later cycle);
subjects at or above the bank's degree percentile (default p99) get interval × 3.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from ..engine.schema import fq_table

logger = logging.getLogger(__name__)

DEFAULT_MIN_REFRESH_SECONDS = 600  # HINDSIGHT_API_OKF_MIN_REFRESH_INTERVAL
HUB_MULTIPLIER = 3


async def _hub_degree_threshold(conn, *, bank_id: str, percentile: int) -> float:
    nodes_t = fq_table("semantic_node")
    edges_t = fq_table("semantic_edge")
    return await conn.fetchval(
        f"""WITH deg AS (
                SELECT n.ref_id, count(*) AS d
                FROM {edges_t} e
                JOIN {nodes_t} n ON n.node_id IN (e.src, e.dst)
                WHERE e.bank_id = $1 AND n.kind = 'entity'
                GROUP BY n.ref_id)
            SELECT coalesce(percentile_cont($2::float / 100) WITHIN GROUP (ORDER BY d), 0)
            FROM deg""",
        bank_id,
        percentile,
    ) or 0.0


async def should_defer_subject(
    conn,
    *,
    bank_id: str,
    subject_id: str,
    min_refresh_seconds: int = DEFAULT_MIN_REFRESH_SECONDS,
    hub_percentile: int = 99,
    _hub_threshold: float | None = None,
) -> bool:
    """True when the subject was projected too recently to justify another run.

    Deferral means: leave the dirty row in place (do NOT project, do NOT
    delete) so it is re-evaluated on the next cycle.
    """
    concepts_t = fq_table("okf_concept")
    nodes_t = fq_table("semantic_node")
    edges_t = fq_table("semantic_edge")

    row = await conn.fetchrow(
        f"SELECT updated_at FROM {concepts_t} WHERE bank_id = $1 AND resource = $2",
        bank_id,
        f"hindsight://{bank_id}/entity/{subject_id}",
    )
    if not row or not row["updated_at"]:
        return False

    interval = min_refresh_seconds
    if _hub_threshold is None:
        _hub_threshold = await _hub_degree_threshold(conn, bank_id=bank_id, percentile=hub_percentile)
    degree = await conn.fetchval(
        f"""SELECT count(*) FROM {edges_t} e
            JOIN {nodes_t} n ON n.node_id IN (e.src, e.dst)
            WHERE e.bank_id = $1 AND n.kind = 'entity' AND n.ref_id = $2::uuid""",
        bank_id,
        subject_id,
    )
    multiplier = HUB_MULTIPLIER if degree and degree >= _hub_threshold > 0 else 1

    updated = row["updated_at"]
    if updated.tzinfo is None:
        updated = updated.replace(tzinfo=UTC)
    fresh_for = (datetime.now(UTC) - updated).total_seconds()
    defer = fresh_for < interval * multiplier
    if defer:
        logger.info(
            f"okf_distill W10 throttle: subject {subject_id} deferred "
            f"(projected {int(fresh_for)}s ago < interval {interval * multiplier}s, degree={degree})"
        )
    return defer
