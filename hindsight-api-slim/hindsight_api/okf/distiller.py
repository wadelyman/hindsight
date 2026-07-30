"""The okf_distill worker job (spec §2.3 stage 8, §5.8).

Claims debounced subjects from ``okf_dirty`` (FOR UPDATE SKIP LOCKED) and runs
the projection subroutines for them. The session GUC
``okf.suppress_dirty = on`` is set for the whole job: any dirty mark attempted
from inside a projection is a recursion bug and the W9 trigger raises loudly
(``okf_recursive_dirty_total`` must remain 0).

Drain-and-resubmit: the debounce window means a burst can leave rows younger
than the claim predicate behind, while op dedupe keeps the burst itself from
enqueueing more. If anything remains after this pass, the job parks one
follow-up op so every subject eventually drains.
"""

from __future__ import annotations

import logging

from ..engine.db_utils import acquire_with_retry
from ..engine.schema import fq_table
from .dirty import submit_okf_distill
from .projectors import project_entity

logger = logging.getLogger(__name__)

DEFAULT_DEBOUNCE_SECONDS = 30
CLAIM_LIMIT = 50


async def run_okf_distill_job(
    *,
    memory_engine,
    bank_id: str,
    request_context,
    operation_id: str | None = None,
    debounce_seconds: int = DEFAULT_DEBOUNCE_SECONDS,
) -> dict:
    """Claim debounced dirty subjects for the bank and project them. Returns a
    stats dict (also written to the op's result_metadata by the caller)."""
    backend = await memory_engine._get_backend()
    dirty = fq_table("okf_dirty")
    stats: dict = {"claimed": 0, "projected": 0, "skipped": 0, "recursive_dirty_violations": 0, "paths": []}

    async with acquire_with_retry(backend) as conn:
        async with conn.transaction():
            # W9: nothing inside this job may enqueue dirty marks.
            await conn.execute("SELECT set_config('okf.suppress_dirty', 'on', true)")

            rows = await conn.fetch(
                f"""SELECT subject_kind, subject_id, reason
                    FROM {dirty}
                    WHERE bank_id = $1
                      AND dirty_since < now() - make_interval(secs => $2::int)
                    ORDER BY dirty_since
                    FOR UPDATE SKIP LOCKED
                    LIMIT $3""",
                bank_id,
                debounce_seconds,
                CLAIM_LIMIT,
            )
            stats["claimed"] = len(rows)

            for row in rows:
                kind = row["subject_kind"]
                subject_id = row["subject_id"]
                result = None
                try:
                    if kind == "entity":
                        result = await project_entity(conn, bank_id=bank_id, entity_id=subject_id)
                    else:
                        logger.info(f"okf_distill: no projector for subject_kind={kind!r} yet (subject {subject_id})")
                except Exception:
                    logger.warning(f"okf_distill: projection failed for {kind}/{subject_id}", exc_info=True)

                if result and not result.get("skipped"):
                    stats["projected"] += 1
                    stats["paths"].append(result.get("path"))
                else:
                    stats["skipped"] += 1

                await conn.execute(
                    f"DELETE FROM {dirty} WHERE bank_id = $1 AND subject_kind = $2 AND subject_id = $3",
                    bank_id,
                    kind,
                    subject_id,
                )

            # Drain-and-resubmit: rows younger than the debounce (or beyond
            # CLAIM_LIMIT) stay queued; park one follow-up op for them.
            remaining = await conn.fetchval(f"SELECT count(*) FROM {dirty} WHERE bank_id = $1", bank_id)
            if remaining:
                stats["resubmitted"] = True
                await submit_okf_distill(conn, bank_id, debounce_seconds=debounce_seconds)

    logger.info(f"okf_distill complete for bank_id={bank_id}: {stats}")
    return stats
