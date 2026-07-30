"""I4-suspect reconciler (C2c): repair concepts flagged by curation propagation.

Entity concepts are re-projected from surviving evidence (which also rewrites
their source set). Concepts with no surviving grounding at all are deprecated
(links stay resolvable per OKF §5.4). The flag clears in both outcomes.
Extracted so it is testable with a bare asyncpg connection.
"""

from __future__ import annotations

import logging
import re

from ..engine.schema import fq_table
from .projectors import project_entity

logger = logging.getLogger(__name__)

_ENTITY_RESOURCE_RE = re.compile(r"^hindsight://[^/]+/entity/([0-9a-f-]{36})$")


async def reconcile_i4_suspects(conn, *, bank_id: str) -> dict:
    """Drain okf_i4_suspect concepts for the bank. Caller provides the transaction."""
    concepts_t = fq_table("okf_concept")
    suspects = await conn.fetch(
        f"SELECT concept_id, path, resource, generated_by FROM {concepts_t} WHERE bank_id = $1 AND okf_i4_suspect",
        bank_id,
    )
    stats = {"reconciled": 0, "reprojected": 0, "deprecated": 0, "cleared": 0}
    for s in suspects:
        m = _ENTITY_RESOURCE_RE.match(s["resource"] or "")
        reconciled = False
        if m:
            result = await project_entity(conn, bank_id=bank_id, entity_id=m.group(1))
            reconciled = bool(result and not result.get("skipped"))
        if reconciled:
            stats["reprojected"] += 1
            stats["cleared"] += 1
        else:
            surviving = await conn.fetchval(
                f"""SELECT count(*) FROM {fq_table('okf_source')} src
                    JOIN {fq_table('memory_units')} u ON u.id = src.memory_id
                    WHERE src.concept_id = $1""",
                s["concept_id"],
            )
            if surviving == 0 and not (s["generated_by"] or "").startswith("human:"):
                await conn.execute(
                    f"UPDATE {concepts_t} SET status = 'deprecated', updated_at = now() WHERE concept_id = $1",
                    s["concept_id"],
                )
                stats["deprecated"] += 1
            else:
                stats["cleared"] += 1
        await conn.execute(
            f"UPDATE {concepts_t} SET okf_i4_suspect = false WHERE concept_id = $1",
            s["concept_id"],
        )
        stats["reconciled"] += 1
    if stats["reconciled"]:
        logger.info(f"okf i4-suspect reconciler for bank_id={bank_id}: {stats}")
    return stats
