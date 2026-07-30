"""Tier 1 — OKF concept resolver (spec §1.2, §4.2; M2 read path).

EXACT resolution only: concept-path match or exact title match. No ranking,
no LLM, no fuzzy scoring — anything fuzzier is Tier 3's job by design. Any
failure degrades silently to Tier 3 (I6): this resolver can make recall faster
and more precise, but it must never make it unavailable.
"""

from __future__ import annotations

import logging

from ..engine.db_utils import acquire_with_retry
from ..engine.schema import fq_table
from .projectors import slugify

logger = logging.getLogger(__name__)


async def tier1_exact_lookup(backend, *, bank_id: str, query: str, limit: int = 5) -> list[dict]:
    """Exact Tier-1 matches for a recall query.

    Matches when the query (or its slug) IS a concept path, a path suffix, or
    an exact (case-insensitive) concept title. Returns [] on no match and on
    any failure — the caller's Tier-3 pipeline is unaffected either way.
    """
    stripped = query.strip()
    if not stripped:
        return []
    slug = slugify(stripped)
    try:
        async with acquire_with_retry(backend) as conn:
            concepts = fq_table("okf_concept")
            rows = await conn.fetch(
                f"""SELECT concept_id, path, type, title, description, tags, status,
                           stale_after, body, read_count
                    FROM {concepts}
                    WHERE bank_id = $1
                      AND status <> 'deprecated'
                      AND (path = $2 OR path = $3 OR path LIKE $4 OR lower(title) = lower($5))
                    ORDER BY read_count DESC, updated_at DESC
                    LIMIT $6""",
                bank_id,
                stripped,  # raw query may already be a concept path
                slug,
                "%/" + slug,
                stripped,
                limit,
            )
            if rows:
                await conn.execute(
                    f"UPDATE {concepts} SET read_count = read_count + 1 WHERE concept_id = ANY($1::uuid[])",
                    [r["concept_id"] for r in rows],
                )
            return [
                {
                    "path": r["path"],
                    "type": r["type"],
                    "title": r["title"],
                    "description": r["description"],
                    "tags": list(r["tags"] or []),
                    "status": r["status"],
                    "stale_after": r["stale_after"].isoformat() if r["stale_after"] else None,
                    "body": r["body"],
                    "read_count": r["read_count"] + 1,
                }
                for r in rows
            ]
    except Exception:
        logger.warning("okf tier-1 lookup failed; degrading to tier 3", exc_info=True)
        return []
