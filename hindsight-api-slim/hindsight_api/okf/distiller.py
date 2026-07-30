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
from .projectors import project_entity, project_mental_model, project_observation_profile

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

    from .metrics import Timer, gauge, inc

    with Timer("okf_distill_duration_seconds", **{"class": "projection"}):
        async with acquire_with_retry(backend) as conn:
            async with conn.transaction():
                # W9: nothing inside this job may enqueue dirty marks.
                await conn.execute("SELECT set_config('okf.suppress_dirty', 'on', true)")

                rows = await conn.fetch(
                    f"""SELECT subject_kind, subject_id, reason, dirty_since
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
                if rows:
                    from datetime import UTC, datetime

                    from .metrics import observe

                    now = datetime.now(UTC)
                    for r in rows:
                        ds = r["dirty_since"]
                        if ds is not None:
                            if ds.tzinfo is None:
                                from datetime import timezone as _tz

                                ds = ds.replace(tzinfo=_tz.utc)
                            observe("okf_dirty_age_seconds", max(0.0, (now - ds).total_seconds()), bank=bank_id)

            for row in rows:
                kind = row["subject_kind"]
                subject_id = row["subject_id"]
                results = []
                try:
                    if kind == "entity":
                        # D3 index concept; D2 profile when observations exist.
                        results.append(await project_entity(conn, bank_id=bank_id, entity_id=subject_id))
                        results.append(await project_observation_profile(conn, bank_id=bank_id, entity_id=subject_id))
                    elif kind == "mental_model":
                        results.append(await project_mental_model(conn, bank_id=bank_id, mental_model_id=subject_id))
                    else:
                        logger.info(f"okf_distill: no projector for subject_kind={kind!r} yet (subject {subject_id})")
                except Exception:
                    logger.warning(f"okf_distill: projection failed for {kind}/{subject_id}", exc_info=True)

                projected_paths = [r.get("path") for r in results if r and not r.get("skipped")]
                if projected_paths:
                    stats["projected"] += 1
                    stats["paths"].extend(projected_paths)
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
            gauge("okf_dirty_depth", float(remaining or 0), bank=bank_id)
            if remaining:
                stats["resubmitted"] = True
                await submit_okf_distill(conn, bank_id, debounce_seconds=debounce_seconds)

            # Materialize concept-derived semantic graph edges for the bank (M4).
            try:
                from .graph import materialize_semantic_graph

                stats["graph"] = await materialize_semantic_graph(conn, bank_id=bank_id)
            except Exception:
                logger.warning("okf semantic graph materialization failed", exc_info=True)

            # M5: offer synthesis for read-hot concepts (opt-in; eligibility is
            # sampled at synthesis run time, never raw-rate queued).
            try:
                from ..config import get_config as _get_config

                if getattr(_get_config(), "okf_synthesis_enabled", False):
                    from .dirty import submit_okf_synthesize

                    await submit_okf_synthesize(conn, bank_id)
            except Exception:
                logger.warning("okf_synthesize submit failed", exc_info=True)

            # C2 reconciler: drain I4-suspect concepts. Entity concepts are
            # re-projected from surviving evidence; concepts with no surviving
            # grounding are deprecated (links stay resolvable per OKF §5.4).
            try:
                import re as _re

                suspects = await conn.fetch(
                    f"SELECT concept_id, path, resource, generated_by FROM {fq_table('okf_concept')} WHERE bank_id = $1 AND okf_i4_suspect",
                    bank_id,
                )
                stats["i4_suspects_reconciled"] = 0
                for s in suspects:
                    m = _re.match(r"^hindsight://[^/]+/entity/([0-9a-f-]{36})$", s["resource"] or "")
                    reconciled = False
                    if m:
                        result = await project_entity(conn, bank_id=bank_id, entity_id=m.group(1))
                        reconciled = bool(result and not result.get("skipped"))
                    if not reconciled:
                        surviving = await conn.fetchval(
                            f"""SELECT count(*) FROM {fq_table('okf_source')} src
                                JOIN {fq_table('memory_units')} u ON u.id = src.memory_id
                                WHERE src.concept_id = $1""",
                            s["concept_id"],
                        )
                        if surviving == 0 and not (s["generated_by"] or "").startswith("human:"):
                            await conn.execute(
                                f"UPDATE {fq_table('okf_concept')} SET status = 'deprecated', updated_at = now() WHERE concept_id = $1",
                                s["concept_id"],
                            )
                    await conn.execute(
                        f"UPDATE {fq_table('okf_concept')} SET okf_i4_suspect = false WHERE concept_id = $1",
                        s["concept_id"],
                    )
                    stats["i4_suspects_reconciled"] += 1
                suspects_left = await conn.fetchval(
                    f"SELECT count(*) FROM {fq_table('okf_concept')} WHERE bank_id = $1 AND okf_i4_suspect",
                    bank_id,
                )
                gauge("okf_i4_suspect_total", float(suspects_left or 0), bank=bank_id)
            except Exception:
                logger.warning("okf i4-suspect reconciler failed", exc_info=True)

        # Section-embedding backfill/forward-fill (§3.7) — runs outside the
        # claim transaction; uses the engine's embedding service.
        try:
            from .embeddings import backfill_section_embeddings

            stats["section_embeddings"] = await backfill_section_embeddings(memory_engine, bank_id=bank_id)
        except Exception:
            logger.warning("section embedding backfill failed", exc_info=True)

    logger.info(f"okf_distill complete for bank_id={bank_id}: {stats}")
    gauge("okf_recursive_dirty_gauge", float(stats["recursive_dirty_violations"]), bank=bank_id)
    gauge("worker_slot_utilization", float(stats["claimed"]) / 2.0, type="okf_distill")
    return stats
