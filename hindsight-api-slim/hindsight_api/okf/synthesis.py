"""Synthesis subroutines (spec §2.4 D6, §6.5 governor, §6.6 circuit breaker).

LLM-driven and OPT-IN (``HINDSIGHT_API_OKF_SYNTHESIS_ENABLED``, default OFF).
Two disciplines from the spec are enforced here, not delegated:

- **Sampled, not queued** (§6.8): only read-hot concepts
  (``read_count >= ELIGIBLE_READ_COUNT``) are eligible, which keeps the
  eligibility rate far below the 15% breaker threshold. Raw-rate queueing of
  LLM work is arithmetically unstable.
- **No LLM-authored authority** (I7): synthesis concepts land at
  ``status: draft`` — they can never self-promote; ``stable`` requires a
  human: or process: verification by design.

The governor (§6.5) also runs here: concepts with zero reads for two TTL
periods are auto-demoted to ``deprecated``.
"""

from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

from pydantic import BaseModel, Field

from ..engine.db_utils import acquire_with_retry
from ..engine.schema import fq_table
from .projectors import PRODUCER, _content_hash, slugify

logger = logging.getLogger(__name__)

ELIGIBLE_READ_COUNT = 2  # read-hot filter: sampled eligibility (§6.8)
GOVERNOR_STALE_DAYS = 60  # two TTL periods (§6.5)
BREAKER_PENDING_FACTOR = 10  # trip when pending(okf_*) > 10 × worker_max_slots
PROMPT_MAX_FACTS = 24


class SynthesizedConcept(BaseModel):
    type: str = Field(description="Short concept type name, e.g. 'Initiative', 'Runbook', 'Decision'")
    title: str
    body: str = Field(description="Markdown body, grounded strictly in the supplied facts")


async def run_okf_synthesize_job(
    *,
    memory_engine,
    bank_id: str,
    request_context,
    operation_id: str | None = None,
) -> dict:
    """D6 synthesize_cross_entity for read-hot subjects (opt-in, sampled)."""
    backend = await memory_engine._get_backend()
    concepts_t = fq_table("okf_concept")
    src_t = fq_table("okf_source")
    ops_t = fq_table("async_operations")
    stats: dict = {"governor_demoted": 0, "eligible": 0, "synthesized": 0, "skipped": 0, "breaker": False}

    async with acquire_with_retry(backend) as conn:
        async with conn.transaction():
            # Governor (§6.5): zero reads for two TTL periods → deprecated.
            demoted = await conn.execute(
                f"""UPDATE {concepts_t}
                    SET status = 'deprecated', updated_at = now()
                    WHERE bank_id = $1 AND status <> 'deprecated'
                      AND read_count = 0 AND updated_at < now() - make_interval(days => $2::int)""",
                bank_id,
                GOVERNOR_STALE_DAYS,
            )
            stats["governor_demoted"] = int(demoted.split()[-1]) if demoted else 0

            # Circuit breaker (§6.6): pending OKF ops beyond 10×C → park and exit.
            from ..config import get_config

            max_slots = int(getattr(get_config(), "worker_max_slots", 10))
            pending = await conn.fetchval(
                f"SELECT count(*) FROM {ops_t} WHERE operation_type LIKE 'okf%' AND status = 'pending'",
            )
            if pending > BREAKER_PENDING_FACTOR * max_slots:
                stats["breaker"] = True
                logger.warning(f"okf_synthesize circuit breaker tripped: {pending} pending okf_* ops (> {BREAKER_PENDING_FACTOR * max_slots})")
                return stats

            # Eligibility (§6.8 sampled): read-hot, projection-built, not stale.
            hot = await conn.fetch(
                f"""SELECT concept_id, path, title, body, read_count
                    FROM {concepts_t}
                    WHERE bank_id = $1 AND status = 'stable' AND distill_class = 'projection'
                      AND read_count >= $2
                      AND (stale_after IS NULL OR stale_after > CURRENT_DATE)
                    ORDER BY read_count DESC
                    LIMIT 5""",
                bank_id,
                ELIGIBLE_READ_COUNT,
            )
            stats["eligible"] = len(hot)

            if not hot:
                return stats

            llm_config = getattr(memory_engine, "_consolidation_llm_config", None) or getattr(memory_engine, "_llm_config", None)
            if llm_config is None:
                logger.warning("okf_synthesize: no LLM config available on memory_engine")
                stats["skipped"] = len(hot)
                return stats

            for concept in hot:
                try:
                    facts = await conn.fetch(
                        f"""SELECT u.text, u.created_at
                            FROM {src_t} s
                            JOIN {fq_table('memory_units')} u ON u.id = s.memory_id
                            WHERE s.concept_id = $1 AND s.memory_id IS NOT NULL
                            ORDER BY u.created_at DESC
                            LIMIT $2""",
                        concept["concept_id"],
                        PROMPT_MAX_FACTS,
                    )
                    if len(facts) < 2:
                        stats["skipped"] += 1
                        continue
                    fact_block = "\n".join(f"- {f['text']}" for f in facts)
                    result = await llm_config.call(
                        messages=[
                            {
                                "role": "user",
                                "content": (
                                    "You are distilling a knowledge concept from memory facts. "
                                    "Produce ONE concept that synthesizes what the facts collectively establish — "
                                    "cross-entity structure only when the facts support it. Never invent details.\n\n"
                                    f"Subject concept: {concept['title']}\n\nFacts:\n{fact_block}"
                                ),
                            }
                        ],
                        response_format=SynthesizedConcept,
                        max_completion_tokens=900,
                        temperature=0.2,
                    )
                    body = result.body.strip()
                    if not body:
                        stats["skipped"] += 1
                        continue

                    path = f"synthesis/{slugify(concept['title'])}"
                    content_hash = _content_hash(result.type, result.title, "", body)
                    await conn.execute(
                        f"""INSERT INTO {concepts_t}
                                (bank_id, path, type, title, description, tags, status,
                                 generated_by, generated_at, body, distill_class, content_hash,
                                 source_generation, okf_version)
                            VALUES ($1,$2,$3,$4,$5,$6,'draft',$7,now(),$8,'synthesis',$9,1,'0.2')
                            ON CONFLICT (bank_id, path) DO UPDATE SET
                                title = EXCLUDED.title,
                                body = EXCLUDED.body,
                                content_hash = EXCLUDED.content_hash,
                                source_generation = {concepts_t}.source_generation + 1,
                                updated_at = now()""",
                        bank_id,
                        path,
                        result.type,
                        result.title,
                        (concept["title"] or "")[:140],
                        ["synthesis"],
                        PRODUCER,
                        body,
                        content_hash,
                    )
                    # Grounding: reuse the subject concept's memory sources (I4).
                    new_id = await conn.fetchval(f"SELECT concept_id FROM {concepts_t} WHERE bank_id=$1 AND path=$2", bank_id, path)
                    await conn.execute(f"DELETE FROM {src_t} WHERE concept_id = $1", new_id)
                    await conn.execute(
                        f"""INSERT INTO {src_t} (concept_id, source_key, resource, title, memory_id)
                            SELECT $1, 'src-' || s.memory_id::text, 'hindsight://' || $2 || '/memory/' || s.memory_id::text,
                                   left(u.text, 80), s.memory_id
                            FROM {src_t} s JOIN {fq_table('memory_units')} u ON u.id = s.memory_id
                            WHERE s.concept_id = $3 AND s.memory_id IS NOT NULL""",
                        new_id,
                        bank_id,
                        concept["concept_id"],
                    )
                    stats["synthesized"] += 1
                except Exception:
                    stats["skipped"] += 1
                    logger.warning(f"okf_synthesize: failed for {concept['path']}", exc_info=True)

    logger.info(f"okf_synthesize complete for bank_id={bank_id}: {stats}")
    return stats
