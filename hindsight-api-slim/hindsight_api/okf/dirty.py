"""OKF stage-7 dirty-marking (spec §2.3/§2.5).

The retain pipeline's write transaction dirty-marks *subjects* (never events)
via a coalescing UPSERT into ``okf_dirty`` — one row per subject, reason
bitmask unioned, the EARLIEST ``dirty_since`` preserved (W5) so a hot subject
can never livelock the debounce. The ``okf_distill`` async op is enqueued in
the SAME transaction, parked ``debounce_seconds`` into the future, so a claim
can never precede the facts it distills.
"""

from __future__ import annotations

import json
import logging
import uuid
from datetime import UTC, datetime, timedelta

from ..engine.schema import fq_table

logger = logging.getLogger(__name__)

# Reason bitmask values (producer-defined; bit 0 reserved).
REASON_FACTS_ADDED = 2
REASON_OBSERVATION_UPDATED = 4
REASON_MENTAL_MODEL_REFRESHED = 8
REASON_CONCEPT_INVALIDATED = 16

_DIRTY_UPSERT = """INSERT INTO {table} (bank_id, subject_kind, subject_id, dirty_since, reason)
VALUES ($1, $2, $3, now(), $4)
ON CONFLICT (bank_id, subject_kind, subject_id)
DO UPDATE SET reason = {table}.reason | EXCLUDED.reason"""


async def mark_dirty(conn, bank_id: str, subjects: list[tuple[str, str]], reason: int) -> int:
    """Coalescing UPSERT into okf_dirty, in the caller's transaction (§2.5).

    ``subjects`` is a list of ``(subject_kind, subject_id)`` pairs. Returns the
    number of marks attempted (input to the coalescing-ratio measurement).
    """
    if not subjects:
        return 0
    table = fq_table("okf_dirty")
    for kind, subject_id in subjects:
        await conn.execute(_DIRTY_UPSERT.format(table=table), bank_id, kind, str(subject_id), reason)
    return len(subjects)


async def submit_okf_distill(conn, bank_id: str, *, debounce_seconds: int) -> str | None:
    """Enqueue one ``okf_distill`` async op for the bank, in the caller's
    transaction, parked ``debounce_seconds`` into the future via
    ``next_retry_at`` (the worker claim query already honors it).

    Deduped against an existing pending op for the bank — that is itself part
    of the coalescing story: a burst of retains yields one claimable op.
    Returns the operation_id, or None when a pending op already exists.
    """
    ops_table = fq_table("async_operations")
    existing = await conn.fetchval(
        f"""SELECT operation_id FROM {ops_table}
            WHERE bank_id = $1 AND operation_type = 'okf_distill' AND status = 'pending'
            LIMIT 1""",
        bank_id,
    )
    if existing:
        return None

    operation_id = uuid.uuid4()
    payload = {"type": "okf_distill", "operation_id": str(operation_id), "bank_id": bank_id}
    parked_at = datetime.now(UTC) + timedelta(seconds=debounce_seconds)
    await conn.execute(
        f"""INSERT INTO {ops_table}
                (operation_id, bank_id, operation_type, result_metadata, status, task_payload, next_retry_at)
            VALUES ($1, $2, 'okf_distill', $3::jsonb, 'pending', $4::jsonb, $5)""",
        operation_id,
        bank_id,
        json.dumps({}),
        json.dumps(payload),
        parked_at,
    )
    logger.info(f"okf_distill task queued for bank_id={bank_id}, operation_id={operation_id}, parked {debounce_seconds}s")
    return str(operation_id)
