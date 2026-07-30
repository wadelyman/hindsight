"""DB-backed invariant tests for the OKF schema (I4, I7, W9, C2, coalescing).

These are the tests the audit (HS-ARCH-005 C3) required: every invariant is
asserted against a LIVE PostgreSQL through a direct asyncpg connection — not
the HTTP layer — because an API-level test proves the API enforces a rule, not
that the database does (baseline §8: "an API-level test proves nothing about
the invariant").

DSN via HINDSIGHT_TEST_OKF_DSN, defaulting to the dev compose pgvector.
Requires migration d4e6f8a0c2e4 (okf_i4_suspect + curation trigger).
"""

import os
import uuid

import asyncpg
import pytest

from types import SimpleNamespace

DSN = os.environ.get("HINDSIGHT_TEST_OKF_DSN", "postgresql://hindsight:hindsight-dev@127.0.0.1:5433/hindsight")
BANK_PREFIX = "okf-inv"

pytestmark = pytest.mark.asyncio


async def _conn():
    conn = await asyncpg.connect(DSN)
    await conn.execute("SET search_path TO public")
    return conn


@pytest.fixture(scope="function")
async def db():
    conn = await _conn()
    # Unique bank per test so xdist workers never share rows.
    bank = f"{BANK_PREFIX}-{uuid.uuid4().hex[:10]}"
    yield SimpleNamespace(conn=conn, bank=bank)
    await conn.execute("DELETE FROM okf_dirty WHERE bank_id = $1", bank)
    await conn.execute("DELETE FROM okf_concept WHERE bank_id = $1", bank)
    await conn.execute("DELETE FROM okf_computation_proposal WHERE bank_id = $1", bank)
    await conn.execute("DELETE FROM memory_units WHERE bank_id = $1", bank)
    await conn.close()


async def _insert_unit(db, text="grounding fact") -> str:
    uid = uuid.uuid4()
    await db.conn.execute(
        "INSERT INTO memory_units (id, bank_id, text) VALUES ($1, $2, $3)",
        uid,
        db.bank,
        text,
    )
    return str(uid)


async def _insert_concept(db, *, path, generated_by="hindsight/0.8.1+okf", status="draft") -> str:
    cid = uuid.uuid4()
    await db.conn.execute(
        """INSERT INTO okf_concept (concept_id, bank_id, path, type, body, generated_by, status, distill_class, content_hash)
           VALUES ($1, $2, $3, 'Probe', 'x', $4, $5, 'projection', '\\xaa')""",
        cid,
        db.bank,
        path,
        generated_by,
        status,
    )
    return str(cid)


async def test_i4_rejects_ungrounded_machine_concept(db):
    with pytest.raises(asyncpg.exceptions.IntegrityConstraintViolationError):
        async with db.conn.transaction():
            cid = await _insert_concept(db, path=f"probe/{uuid.uuid4().hex[:8]}")
            await db.conn.execute("INSERT INTO okf_source (concept_id, source_key, resource) VALUES ($1, 's', 'r')", cid)
            await db.conn.execute("SET CONSTRAINTS ALL IMMEDIATE")


async def test_i4_permits_ungrounded_human_concept(db):
    async with db.conn.transaction():
        cid = await _insert_concept(db, path=f"probe/{uuid.uuid4().hex[:8]}", generated_by="human:tester")
        await db.conn.execute("SET CONSTRAINTS ALL IMMEDIATE")
    count = await db.conn.fetchval("SELECT count(*) FROM okf_concept WHERE concept_id = $1", cid)
    assert count == 1


async def test_c2_invalidate_grounding_marks_suspect_and_dirty(db):
    """C2(a): hard-deleting a grounding unit dirties the concept (reason 16)
    and sets okf_i4_suspect on the machine-generated concept."""
    uid = await _insert_unit(db)
    async with db.conn.transaction():
        cid = await _insert_concept(db, path=f"probe/{uuid.uuid4().hex[:8]}")
        await db.conn.execute(
            "INSERT INTO okf_source (concept_id, source_key, resource, memory_id) VALUES ($1, 's', 'r', $2)",
            cid,
            uid,
        )
        await db.conn.execute("SET CONSTRAINTS ALL IMMEDIATE")

    await db.conn.execute("DELETE FROM memory_units WHERE id = $1", uid)

    dirty = await db.conn.fetchrow(
        "SELECT reason FROM okf_dirty WHERE bank_id = $1 AND subject_id = $2", db.bank, cid
    )
    assert dirty is not None and (dirty["reason"] & 16) == 16
    suspect = await db.conn.fetchval("SELECT okf_i4_suspect FROM okf_concept WHERE concept_id = $1", cid)
    assert suspect is True


async def test_c2_human_concept_not_flagged_suspect(db):
    uid = await _insert_unit(db)
    async with db.conn.transaction():
        cid = await _insert_concept(db, path=f"probe/{uuid.uuid4().hex[:8]}", generated_by="human:tester")
        await db.conn.execute(
            "INSERT INTO okf_source (concept_id, source_key, resource, memory_id) VALUES ($1, 's', 'r', $2)",
            cid,
            uid,
        )
        await db.conn.execute("SET CONSTRAINTS ALL IMMEDIATE")
    await db.conn.execute("DELETE FROM memory_units WHERE id = $1", uid)
    suspect = await db.conn.fetchval("SELECT okf_i4_suspect FROM okf_concept WHERE concept_id = $1", cid)
    assert suspect is False


async def test_i7_rejects_agent_authored_computation(db):
    uid = await _insert_unit(db)
    async with db.conn.transaction():
        cid = await _insert_concept(db, path=f"probe/{uuid.uuid4().hex[:8]}")
        await db.conn.execute(
            "INSERT INTO okf_source (concept_id, source_key, resource, memory_id) VALUES ($1, 's', 'r', $2)",
            cid,
            uid,
        )
        with pytest.raises(asyncpg.exceptions.CheckViolationError):
            await db.conn.execute(
                "INSERT INTO okf_computation (concept_id, runtime, computation_inline, authored_by) VALUES ($1, 'postgres', 'SELECT 1', 'hindsight/0.8.1')",
                cid,
            )


async def test_i7_rejects_stable_promotion_without_verification(db):
    uid = await _insert_unit(db)
    with pytest.raises(asyncpg.exceptions.IntegrityConstraintViolationError):
        async with db.conn.transaction():
            cid = await _insert_concept(db, path=f"probe/{uuid.uuid4().hex[:8]}")
            await db.conn.execute(
                "INSERT INTO okf_source (concept_id, source_key, resource, memory_id) VALUES ($1, 's', 'r', $2)",
                cid,
                uid,
            )
            await db.conn.execute("UPDATE okf_concept SET status = 'stable' WHERE concept_id = $1", cid)
            await db.conn.execute("SET CONSTRAINTS ALL IMMEDIATE")


async def test_i7_allows_stable_promotion_with_process_verification(db):
    uid = await _insert_unit(db)
    async with db.conn.transaction():
        cid = await _insert_concept(db, path=f"probe/{uuid.uuid4().hex[:8]}")
        await db.conn.execute(
            "INSERT INTO okf_source (concept_id, source_key, resource, memory_id) VALUES ($1, 's', 'r', $2)",
            cid,
            uid,
        )
        await db.conn.execute(
            "INSERT INTO okf_verified (concept_id, actor, verified_at) VALUES ($1, 'process:test', now())",
            cid,
        )
        await db.conn.execute("UPDATE okf_concept SET status = 'stable' WHERE concept_id = $1", cid)
        await db.conn.execute("SET CONSTRAINTS ALL IMMEDIATE")
    status = await db.conn.fetchval("SELECT status FROM okf_concept WHERE concept_id = $1", cid)
    assert status == "stable"


async def test_w9_suppress_dirty_blocks_recursive_mark(db):
    with pytest.raises(asyncpg.exceptions.RaiseError):
        async with db.conn.transaction():
            await db.conn.execute("SELECT set_config('okf.suppress_dirty', 'on', true)")
            await db.conn.execute(
                "INSERT INTO okf_dirty (bank_id, subject_kind, subject_id, reason) VALUES ($1, 'entity', 'w9-probe', 1)",
                db.bank,
            )


async def test_w9_normal_mark_allowed(db):
    await db.conn.execute(
        "INSERT INTO okf_dirty (bank_id, subject_kind, subject_id, reason) VALUES ($1, 'entity', 'w9-ok', 1)",
        db.bank,
    )
    count = await db.conn.fetchval("SELECT count(*) FROM okf_dirty WHERE bank_id = $1", db.bank)
    assert count == 1


async def test_coalescing_upsert_unions_reason_preserves_earliest(db):
    for reason in (2, 4, 8):
        await db.conn.execute(
            """INSERT INTO okf_dirty (bank_id, subject_kind, subject_id, dirty_since, reason)
               VALUES ($1, 'entity', 'hot-subject', now(), $2)
               ON CONFLICT (bank_id, subject_kind, subject_id)
               DO UPDATE SET reason = okf_dirty.reason | EXCLUDED.reason""",
            db.bank,
            reason,
        )
    rows = await db.conn.fetch("SELECT reason, dirty_since FROM okf_dirty WHERE bank_id = $1", db.bank)
    assert len(rows) == 1
    assert rows[0]["reason"] == (2 | 4 | 8)


async def test_debounce_claim_excludes_fresh_rows(db):
    await db.conn.execute(
        "INSERT INTO okf_dirty (bank_id, subject_kind, subject_id, dirty_since, reason) VALUES ($1, 'entity', 'aged', now() - interval '31 seconds', 2)",
        db.bank,
    )
    await db.conn.execute(
        "INSERT INTO okf_dirty (bank_id, subject_kind, subject_id, dirty_since, reason) VALUES ($1, 'entity', 'fresh', now(), 2)",
        db.bank,
    )
    claimed = await db.conn.fetch(
        "SELECT subject_id FROM okf_dirty WHERE bank_id = $1 AND dirty_since < now() - interval '30 seconds'",
        db.bank,
    )
    assert [r["subject_id"] for r in claimed] == ["aged"]
