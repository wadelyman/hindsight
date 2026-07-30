"""OKF: extension-aware vector indexes (C1) + curation propagation (C2).

C1 — c3e5f7a9b1d4 hardcoded `USING hnsw (embedding vector_cosine_ops)`, correct
only for pgvector. Three of four supported extensions (vchord, pgvectorscale,
scann) either lack `hnsw` or require a different method/opclass. This revision
rebuilds both OKF vector indexes through the shared
``hindsight_api._vector_index.index_using_clause`` dispatcher (the same helper
every newer revision uses), with ScaNN's empty-table deferral, and renames the
indexes method-agnostically (`*_vec_idx` — `*_hnsw_idx` is a lie on three of
four backends).

C2 — grounding decay at READ time. Invariant I4 holds at write time but can go
false at read time: when memory units are hard-deleted (document cascade on
re-retain, document deletion, retention sweeps), `okf_source.memory_id`
goes NULL via ON DELETE SET NULL and a machine-generated concept can end up
ungrounded while still being injected as authoritative. This revision:
  * adds `okf_i4_suspect` (+ partial index) to mark concepts whose grounding
    may no longer resolve;
  * installs a BEFORE DELETE trigger on memory_units that dirty-marks every
    dependent concept (REASON_CONCEPT_INVALIDATED = 16 — the previously
    declared-but-unwired reason bit) and flags machine-generated concepts left
    with zero surviving grounding sources;
  * installs the same propagation for the soft-curation columns
    (`invalidated_at`, `edited_at`) **only when they exist** — 0.8.2+ adds
    them; on 0.8.1 the block is a documented no-op.
The trigger deliberately never RAISEs: a user retracting a fact must always
succeed (I6, and OKF must never break a core operation). The violation is
recorded and repaired asynchronously by the distill cycle's reconciler.

PostgreSQL only.

Revision ID: d4e6f8a0c2e4
Revises: c3e5f7a9b1d4
Create Date: 2026-07-30
"""

from collections.abc import Sequence

from alembic import context, op
from sqlalchemy import text

from hindsight_api._vector_index import (
    configured_vector_extension,
    index_using_clause,
    should_defer_index_creation,
)
from hindsight_api.alembic._dialect import run_for_dialect

revision: str = "d4e6f8a0c2e4"
down_revision: str | Sequence[str] | None = "c3e5f7a9b1d4"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None

_OKF_VECTOR_TABLES = ("okf_concept", "okf_concept_section")
_LEGACY_INDEX_NAMES = ("okf_concept_hnsw_idx", "okf_section_hnsw_idx")


def _get_schema_prefix() -> str:
    """Get schema prefix for table names (required for multi-tenant support)."""
    schema = context.config.get_main_option("target_schema")
    return f'"{schema}".' if schema else ""


def _pg_upgrade() -> None:
    s = _get_schema_prefix()
    ext = configured_vector_extension()
    using = index_using_clause(ext)

    # ------------------------------------------------------------------ C1 --
    for table in _OKF_VECTOR_TABLES:
        for legacy in _LEGACY_INDEX_NAMES:
            op.execute(f"DROP INDEX IF EXISTS {s}{legacy}")
        op.execute(f"DROP INDEX IF EXISTS {s}{table}_vec_idx")
        # ScaNN refuses AUTO indexes on empty tables; defer creation until rows
        # exist (matches n9i0j1k2l3m4's deferral convention).
        row_count = op.get_bind().execute(text(f"SELECT count(*) FROM {s}{table}")).scalar()
        if should_defer_index_creation(ext, row_count):
            continue
        op.execute(f"CREATE INDEX {table}_vec_idx ON {s}{table} {using}")

    # ------------------------------------------------------------------ C2 --
    op.execute(f"""
        ALTER TABLE {s}okf_concept
        ADD COLUMN okf_i4_suspect boolean NOT NULL DEFAULT false
    """)
    op.execute(f"""
        CREATE INDEX okf_concept_i4_suspect_idx ON {s}okf_concept (bank_id)
        WHERE okf_i4_suspect
    """)

    # Propagation function: dirty-mark dependents (reason 16) and flag concepts
    # whose machine-generated grounding no longer resolves. Never raises.
    # NOTE: must fire BEFORE the delete, because ON DELETE SET NULL clears
    # okf_source.memory_id first — an AFTER trigger would find no dependents.
    op.execute(f"""
        CREATE OR REPLACE FUNCTION {s}okf_propagate_curation() RETURNS trigger
        LANGUAGE plpgsql AS $fn$
        DECLARE
            target_id uuid;
        BEGIN
            target_id := COALESCE(NEW.id, OLD.id);

            INSERT INTO {s}okf_dirty (bank_id, subject_kind, subject_id, dirty_since, reason)
            SELECT DISTINCT c.bank_id, 'concept', c.concept_id::text, now(), 16
              FROM {s}okf_source src
              JOIN {s}okf_concept c ON c.concept_id = src.concept_id
             WHERE src.memory_id = target_id
            ON CONFLICT (bank_id, subject_kind, subject_id)
            DO UPDATE SET reason = {s}okf_dirty.reason | EXCLUDED.reason;

            -- Flag machine-generated concepts left with zero OTHER surviving
            -- grounding units (the unit being deleted still exists at BEFORE
            -- time, so it must be excluded explicitly). This is the read-time
            -- I4 violation; record it, do not raise (retraction must succeed).
            UPDATE {s}okf_concept c
               SET okf_i4_suspect = true
             WHERE c.generated_by NOT LIKE 'human:%'
               AND EXISTS (SELECT 1 FROM {s}okf_source s1 WHERE s1.concept_id = c.concept_id AND s1.memory_id = target_id)
               AND NOT EXISTS (
                     SELECT 1
                       FROM {s}okf_source s2
                       JOIN {s}memory_units m2 ON m2.id = s2.memory_id
                      WHERE s2.concept_id = c.concept_id
                        AND s2.memory_id <> target_id);
            RETURN COALESCE(NEW, OLD);
        END $fn$
    """)

    # Hard deletes exist in every supported version (document cascade on
    # re-retain, document deletion, retention sweeps). BEFORE DELETE so the
    # trigger runs before the FK's ON DELETE SET NULL action.
    op.execute(f"""
        CREATE TRIGGER memory_units_okf_curation_delete
            BEFORE DELETE ON {s}memory_units
            FOR EACH ROW EXECUTE FUNCTION {s}okf_propagate_curation()
    """)

    # Soft-curation columns (invalidated_at, edited_at) arrive with 0.8.2's
    # reversible curation. Install the same propagation when they exist;
    # on 0.8.1 this block intentionally does nothing.
    op.execute(f"""
        DO $$
        BEGIN
            IF EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'memory_units' AND column_name = 'invalidated_at'
            ) AND EXISTS (
                SELECT 1 FROM information_schema.columns
                WHERE table_name = 'memory_units' AND column_name = 'edited_at'
            ) THEN
                EXECUTE 'CREATE TRIGGER memory_units_okf_curation_update
                    AFTER UPDATE OF invalidated_at, edited_at ON {s}memory_units
                    FOR EACH ROW EXECUTE FUNCTION {s}okf_propagate_curation()';
            END IF;
        END $$
    """)


def _pg_downgrade() -> None:
    s = _get_schema_prefix()
    ext = configured_vector_extension()
    using = index_using_clause(ext)

    op.execute(f"DROP TRIGGER IF EXISTS memory_units_okf_curation_update ON {s}memory_units")
    op.execute(f"DROP TRIGGER IF EXISTS memory_units_okf_curation_delete ON {s}memory_units")
    op.execute(f"DROP FUNCTION IF EXISTS {s}okf_propagate_curation()")
    op.execute(f"DROP INDEX IF EXISTS {s}okf_concept_i4_suspect_idx")
    op.execute(f"ALTER TABLE {s}okf_concept DROP COLUMN IF EXISTS okf_i4_suspect")

    for table in _OKF_VECTOR_TABLES:
        op.execute(f"DROP INDEX IF EXISTS {s}{table}_vec_idx")
    op.execute(f"""CREATE INDEX okf_concept_hnsw_idx ON {s}okf_concept
        USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)""")
    op.execute(f"""CREATE INDEX okf_section_hnsw_idx ON {s}okf_concept_section
        USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)""")


def upgrade() -> None:
    run_for_dialect(pg=_pg_upgrade)


def downgrade() -> None:
    run_for_dialect(pg=_pg_downgrade)
