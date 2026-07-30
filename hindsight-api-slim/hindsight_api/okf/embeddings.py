"""Section-embedding backfill (spec §3.7 dimensional parity, M4/M5 hygiene).

``okf_concept_section.embedding`` starts NULL; this job encodes section bodies
with the engine's own embedding service (same model, same dimension as
``memory_units.embedding`` — that is what keeps Tier-1 vector search inside
one embedding space). It runs inside the distill cycle, so it is both the
one-time backfill AND the forward-fill for sections written by later runs.
"""

from __future__ import annotations

import logging

from ..engine.db_utils import acquire_with_retry
from ..engine.schema import fq_table

logger = logging.getLogger(__name__)

BATCH_SIZE = 64
BATCH_LIMIT = 500


async def backfill_section_embeddings(memory_engine, *, bank_id: str, batch_size: int = BATCH_SIZE, limit: int = BATCH_LIMIT) -> dict:
    """Encode up to ``limit`` section rows missing embeddings for the bank."""
    embeddings = getattr(memory_engine, "embeddings", None)
    if embeddings is None:
        return {"updated": 0, "skipped": "no_embeddings_service"}

    backend = await memory_engine._get_backend()
    sections_t = fq_table("okf_concept_section")
    concepts_t = fq_table("okf_concept")

    async with acquire_with_retry(backend) as conn:
        rows = await conn.fetch(
            f"""SELECT s.concept_id, s.ordinal, s.body
                FROM {sections_t} s
                JOIN {concepts_t} c ON c.concept_id = s.concept_id
                WHERE s.embedding IS NULL AND c.bank_id = $1
                ORDER BY s.concept_id, s.ordinal
                LIMIT $2""",
            bank_id,
            limit,
        )
        if not rows:
            return {"updated": 0}

        updated = 0
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            vectors = embeddings.encode_documents([r["body"] for r in batch])
            for r, v in zip(batch, vectors):
                await conn.execute(
                    f"UPDATE {sections_t} SET embedding = $3::vector WHERE concept_id = $1 AND ordinal = $2",
                    r["concept_id"],
                    r["ordinal"],
                    str(list(v)),
                )
            updated += len(batch)

    if updated:
        logger.info(f"section embeddings backfilled for bank_id={bank_id}: {updated} sections")
    return {"updated": updated}
