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


async def test_i4_suspect_suppressed_from_tier1(db):
    """C2(b): the suspect flag is inert unless the read path honours it — this
    is the assertion that makes C2 a user-visible guarantee, not a DB annotation."""
    from hindsight_api.okf.resolver import tier1_exact_lookup

    uid = await _insert_unit(db, "suspect probe fact")
    title = f"SuspProbe-{uuid.uuid4().hex[:8]}"
    async with db.conn.transaction():
        cid = await _insert_concept(db, path=f"entities/{title.lower()}")
        await db.conn.execute("UPDATE okf_concept SET title = $1 WHERE concept_id = $2", title, cid)
        await db.conn.execute(
            "INSERT INTO okf_source (concept_id, source_key, resource, memory_id) VALUES ($1, 's', 'r', $2)",
            cid,
            uid,
        )
        await db.conn.execute("SET CONSTRAINTS ALL IMMEDIATE")

    pool = await asyncpg.create_pool(DSN)
    try:
        hits = await tier1_exact_lookup(pool, bank_id=db.bank, query=title)
        assert any(h["path"].endswith(title.lower()) for h in hits), "grounded concept must resolve at Tier 1"

        await db.conn.execute("DELETE FROM memory_units WHERE id = $1", uid)

        hits = await tier1_exact_lookup(pool, bank_id=db.bank, query=title)
        assert not any(h["path"].endswith(title.lower()) for h in hits), "suspect concept must be suppressed (default policy)"

        hits = await tier1_exact_lookup(pool, bank_id=db.bank, query=title, suspect_policy="downgrade")
        assert any(h.get("trust_tier") == "unverified" and h.get("okf_i4_suspect") for h in hits), (
            "downgrade policy must return the suspect with trust forced to unverified"
        )
    finally:
        await pool.close()


async def test_i4_reconciler_deprecates_when_no_grounding_survives(db):
    """C2(c): the reconciler deprecates a machine-generated concept whose
    grounding is fully retracted, and clears the suspect flag."""
    from hindsight_api.okf.reconcile import reconcile_i4_suspects

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
    assert await db.conn.fetchval("SELECT okf_i4_suspect FROM okf_concept WHERE concept_id = $1", cid) is True

    async with db.conn.transaction():
        stats = await reconcile_i4_suspects(db.conn, bank_id=db.bank)
    assert stats["deprecated"] == 1
    row = await db.conn.fetchrow("SELECT status, okf_i4_suspect FROM okf_concept WHERE concept_id = $1", cid)
    assert row["status"] == "deprecated"
    assert row["okf_i4_suspect"] is False


async def test_coalescing_amortization_bound(db):
    """The O(w/d) bound (§2.5): a 50-fact burst on one subject yields ONE dirty
    row and ONE deduped op — not 50 of either."""
    from hindsight_api.okf.dirty import submit_okf_distill

    await db.conn.execute(
        "INSERT INTO banks (bank_id, name) VALUES ($1, $1) ON CONFLICT (bank_id) DO NOTHING",
        db.bank,
    )
    for _ in range(50):
        await db.conn.execute(
            """INSERT INTO okf_dirty (bank_id, subject_kind, subject_id, dirty_since, reason)
               VALUES ($1, 'entity', 'burst-hot', now(), 2)
               ON CONFLICT (bank_id, subject_kind, subject_id)
               DO UPDATE SET reason = okf_dirty.reason | EXCLUDED.reason""",
            db.bank,
        )
    rows = await db.conn.fetch("SELECT * FROM okf_dirty WHERE bank_id = $1", db.bank)
    assert len(rows) == 1

    async with db.conn.transaction():
        op1 = await submit_okf_distill(db.conn, db.bank, debounce_seconds=30)
    async with db.conn.transaction():
        op2 = await submit_okf_distill(db.conn, db.bank, debounce_seconds=30)
    assert op1 is not None and op2 is None, "op dedupe must yield exactly one claimable op per bank"


async def test_w10_hub_subject_deferred_normal_subject_not(db):
    """W10 skew: a freshly-projected hub entity is deferred by the throttle;
    a stale one is not. Uniform workloads never surface this — real ones always do."""
    from hindsight_api.okf.throttle import should_defer_subject

    hub_id = str(uuid.uuid4())
    async with db.conn.transaction():
        await db.conn.execute(
            "INSERT INTO entities (id, bank_id, canonical_name, first_seen, last_seen) VALUES ($1, $2, 'hub-entity', now(), now())",
            hub_id,
            db.bank,
        )
        hub_cid = await db.conn.fetchval(
            """INSERT INTO okf_concept (bank_id, path, type, body, generated_by, status, distill_class, content_hash, resource, updated_at)
               VALUES ($1, 'entities/hub-entity', 'Entity', 'x', 'hindsight/0.8.1', 'draft', 'projection', '\\xaa', $2, now())
               RETURNING concept_id""",
            db.bank,
            f"hindsight://{db.bank}/entity/{hub_id}",
        )
        hub_uid = await db.conn.fetchval(
            "INSERT INTO memory_units (id, bank_id, text) VALUES (gen_random_uuid(), $1, 'hub grounding') RETURNING id",
            db.bank,
        )
        await db.conn.execute(
            "INSERT INTO okf_source (concept_id, source_key, resource, memory_id) VALUES ($1, 's', 'r', $2)",
            hub_cid,
            hub_uid,
        )
        # hub node with 50 outgoing edges (degree ≥ p99 for this bank)
        nid = await db.conn.fetchval(
            "INSERT INTO semantic_node (bank_id, kind, ref_id) VALUES ($1, 'entity', $2) RETURNING node_id",
            db.bank,
            hub_id,
        )
        await db.conn.execute(
            "INSERT INTO semantic_node (bank_id, kind, ref_id) SELECT $1, 'entity', gen_random_uuid() FROM generate_series(1, 50) ON CONFLICT DO NOTHING",
            db.bank,
        )
        await db.conn.execute(
            "INSERT INTO semantic_edge (bank_id, src, dst, predicate, provenance) SELECT $1, $2, node_id, 'okf:link', 'linked' FROM semantic_node WHERE bank_id = $1 AND kind = 'entity' AND node_id <> $2",
            db.bank,
            nid,
        )

    assert await should_defer_subject(db.conn, bank_id=db.bank, subject_id=hub_id, min_refresh_seconds=600, hub_percentile=99) is True, (
        "freshly-projected hub subject must be deferred"
    )

    stale_id = str(uuid.uuid4())
    async with db.conn.transaction():
        await db.conn.execute(
            "INSERT INTO entities (id, bank_id, canonical_name, first_seen, last_seen) VALUES ($1, $2, 'stale-entity', now(), now())",
            stale_id,
            db.bank,
        )
        stale_cid = await db.conn.fetchval(
            """INSERT INTO okf_concept (bank_id, path, type, body, generated_by, status, distill_class, content_hash, resource, updated_at)
               VALUES ($1, 'entities/stale-entity', 'Entity', 'x', 'hindsight/0.8.1', 'draft', 'projection', '\\xaa', $2, now() - interval '2 hours')
               RETURNING concept_id""",
            db.bank,
            f"hindsight://{db.bank}/entity/{stale_id}",
        )
        stale_uid = await db.conn.fetchval(
            "INSERT INTO memory_units (id, bank_id, text) VALUES (gen_random_uuid(), $1, 'stale grounding') RETURNING id",
            db.bank,
        )
        await db.conn.execute(
            "INSERT INTO okf_source (concept_id, source_key, resource, memory_id) VALUES ($1, 's', 'r', $2)",
            stale_cid,
            stale_uid,
        )
    assert await should_defer_subject(db.conn, bank_id=db.bank, subject_id=stale_id, min_refresh_seconds=600, hub_percentile=99) is False, (
        "stale subject must not be deferred"
    )
