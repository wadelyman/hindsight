"""OKF concept store + semantic graph (Phase 4 "0041").

Additive-only migration per design invariant I3 (no ALTER/DROP/SET NOT NULL on
pre-existing objects; rollback = downgrade or feature-flag flip).

Creates the OKF v0.2 concept store (``okf_concept`` + ``okf_verified`` +
``okf_source`` + ``okf_computation`` + ``okf_concept_section``), the unified
ontological graph (``semantic_node`` / ``semantic_edge``), the bundle registry
(``okf_bundle`` / ``okf_bundle_log``), the coalescing distill queue
(``okf_dirty``), the human-gated proposal table (``okf_computation_proposal``),
the I4/I7 constraint triggers, and the W9 session-GUC guard on ``okf_dirty``.

Ground-truthed against a live 0.8.1 deployment (see
``plans/hindsight-integration/phase-4-okf-semantic-layer.md`` §0.5):
- FK target is ``memory_units(id)`` (uuid PK), not ``memories``.
- Embedding columns are created at ``vector(384)`` matching the codebase-wide
  baseline; ``ensure_embedding_dimension`` reconciles non-384 models at
  startup (this revision registers both OKF embedding tables for that pass).
- The I4/I7 triggers carry an existence guard so an insert+delete within one
  transaction is not a spurious violation (found by the M0 test suite).
- The reserved-path CHECK rejects ``(^|/)(index|log)\\.md(/|$)`` in addition to
  final ``index``/``log`` segments.

PostgreSQL only — the OKF/semantic surface is not wired for Oracle in this
revision, so the Oracle slot is intentionally absent.

Revision ID: c3e5f7a9b1d4
Revises: b2d4f6a8c1e3
Create Date: 2026-07-29
"""

from collections.abc import Sequence

from alembic import context, op

from hindsight_api.alembic._dialect import run_for_dialect

revision: str = "c3e5f7a9b1d4"
down_revision: str | Sequence[str] | None = "b2d4f6a8c1e3"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def _get_schema_prefix() -> str:
    """Get schema prefix for table names (required for multi-tenant support)."""
    schema = context.config.get_main_option("target_schema")
    return f'"{schema}".' if schema else ""


def _pg_upgrade() -> None:
    s = _get_schema_prefix()

    # -- Enums ----------------------------------------------------------------
    op.execute(f"CREATE TYPE {s}okf_status AS ENUM ('draft', 'stable', 'deprecated')")
    op.execute(f"CREATE TYPE {s}okf_distill_class AS ENUM ('projection', 'synthesis')")

    # -- Concept store ---------------------------------------------------------
    op.execute(f"""
        CREATE TABLE {s}okf_concept (
            concept_id      uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            bank_id         text        NOT NULL,
            path            text        NOT NULL,
            type            text        NOT NULL,
            title           text,
            description     text,
            resource        text,
            tags            text[]      NOT NULL DEFAULT '{{}}',
            status          {s}okf_status NOT NULL DEFAULT 'draft',
            stale_after     date,
            generated_by    text        NOT NULL,
            generated_at    timestamptz NOT NULL DEFAULT now(),
            extensions      jsonb       NOT NULL DEFAULT '{{}}'::jsonb,
            body            text        NOT NULL,
            body_tsv        tsvector GENERATED ALWAYS AS (
                                to_tsvector('english',
                                    coalesce(title,'') || ' ' ||
                                    coalesce(description,'') || ' ' || body)
                            ) STORED,
            embedding       vector(384),
            distill_class   {s}okf_distill_class NOT NULL,
            content_hash    bytea       NOT NULL,
            source_generation bigint  NOT NULL DEFAULT 0,
            read_count      bigint      NOT NULL DEFAULT 0,
            invalidation_count bigint   NOT NULL DEFAULT 0,
            okf_version     text        NOT NULL DEFAULT '0.2',
            created_at      timestamptz NOT NULL DEFAULT now(),
            updated_at      timestamptz NOT NULL DEFAULT now(),
            CONSTRAINT okf_concept_path_uq   UNIQUE (bank_id, path),
            CONSTRAINT okf_type_nonempty     CHECK (length(btrim(type)) > 0),
            CONSTRAINT okf_path_not_reserved CHECK (
                path !~ '(^|/)(index|log)$'
                AND path !~ '(^|/)(index|log)\\.md(/|$)'
            ),
            CONSTRAINT okf_path_shape        CHECK (
                path ~ '^[A-Za-z0-9._-]+(/[A-Za-z0-9._-]+)*$' AND path NOT LIKE '%..%'
            )
        )
    """)
    op.execute(f"""CREATE INDEX okf_concept_bank_type_idx ON {s}okf_concept (bank_id, type)
        WHERE status <> 'deprecated'""")
    op.execute(f"CREATE INDEX okf_concept_tags_idx ON {s}okf_concept USING gin (tags)")
    op.execute(f"CREATE INDEX okf_concept_tsv_idx ON {s}okf_concept USING gin (body_tsv)")
    op.execute(f"""CREATE INDEX okf_concept_hnsw_idx ON {s}okf_concept
        USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)""")
    op.execute(f"""CREATE INDEX okf_concept_stale_idx ON {s}okf_concept (bank_id, stale_after)
        WHERE stale_after IS NOT NULL""")
    op.execute(f"CREATE INDEX okf_concept_ext_idx ON {s}okf_concept USING gin (extensions jsonb_path_ops)")

    op.execute(f"""
        CREATE TABLE {s}okf_verified (
            concept_id  uuid        NOT NULL REFERENCES {s}okf_concept(concept_id) ON DELETE CASCADE,
            actor       text        NOT NULL,
            verified_at timestamptz NOT NULL,
            PRIMARY KEY (concept_id, actor, verified_at)
        )
    """)
    op.execute(f"""CREATE INDEX okf_verified_human_idx ON {s}okf_verified (concept_id)
        WHERE actor LIKE 'human:%'""")

    op.execute(f"""
        CREATE TABLE {s}okf_source (
            concept_id    uuid   NOT NULL REFERENCES {s}okf_concept(concept_id) ON DELETE CASCADE,
            source_key    text   NOT NULL,
            resource      text   NOT NULL,
            title         text,
            author        text,
            usage_count   bigint,
            last_modified date,
            usage_from    date,
            usage_to      date,
            memory_id     uuid REFERENCES {s}memory_units(id) ON DELETE SET NULL,
            PRIMARY KEY (concept_id, source_key),
            CONSTRAINT okf_source_window_order CHECK (
                usage_from IS NULL OR usage_to IS NULL OR usage_from <= usage_to
            )
        )
    """)
    op.execute(f"""CREATE INDEX okf_source_memory_idx ON {s}okf_source (memory_id)
        WHERE memory_id IS NOT NULL""")

    op.execute(f"""
        CREATE TABLE {s}okf_computation (
            concept_id        uuid PRIMARY KEY REFERENCES {s}okf_concept(concept_id) ON DELETE CASCADE,
            runtime           text   NOT NULL,
            parameters        jsonb  NOT NULL DEFAULT '[]'::jsonb,
            computation_path  text,
            computation_inline text,
            executor_resource text,
            executor_receipt  text[]  NOT NULL DEFAULT '{{}}',
            attester_resource text,
            authored_by       text   NOT NULL,
            CONSTRAINT okf_comp_one_form CHECK (
                (computation_path IS NULL) <> (computation_inline IS NULL)
            ),
            CONSTRAINT okf_comp_not_agent_authored CHECK (
                authored_by LIKE 'human:%' OR authored_by LIKE 'process:%'
            )
        )
    """)

    op.execute(f"""
        CREATE TABLE {s}okf_concept_section (
            concept_id  uuid    NOT NULL REFERENCES {s}okf_concept(concept_id) ON DELETE CASCADE,
            ordinal     int     NOT NULL,
            heading     text,
            body        text    NOT NULL,
            token_count int     NOT NULL,
            embedding   vector(384),
            PRIMARY KEY (concept_id, ordinal)
        )
    """)
    op.execute(f"""CREATE INDEX okf_section_hnsw_idx ON {s}okf_concept_section
        USING hnsw (embedding vector_cosine_ops) WITH (m = 16, ef_construction = 64)""")

    # -- Semantic graph ---------------------------------------------------------
    op.execute(f"CREATE TYPE {s}semantic_node_kind AS ENUM ('memory','concept','mental_model','entity')")
    op.execute(f"""
        CREATE TABLE {s}semantic_node (
            node_id  bigserial PRIMARY KEY,
            bank_id  text  NOT NULL,
            kind     {s}semantic_node_kind NOT NULL,
            ref_id   uuid  NOT NULL,
            UNIQUE (bank_id, kind, ref_id)
        )
    """)
    op.execute(f"""
        CREATE TABLE {s}semantic_edge (
            bank_id     text    NOT NULL,
            src         bigint  NOT NULL REFERENCES {s}semantic_node(node_id) ON DELETE CASCADE,
            dst         bigint  NOT NULL REFERENCES {s}semantic_node(node_id) ON DELETE CASCADE,
            predicate   text    NOT NULL,
            weight      real    NOT NULL DEFAULT 1.0 CHECK (weight >= 0 AND weight <= 1),
            provenance  text    NOT NULL,
            valid_from  timestamptz,
            valid_to    timestamptz,
            PRIMARY KEY (src, dst, predicate),
            CONSTRAINT semantic_edge_interval CHECK (
                valid_from IS NULL OR valid_to IS NULL OR valid_from <= valid_to
            )
        )
    """)
    op.execute(f"CREATE INDEX semantic_edge_fwd_idx ON {s}semantic_edge (src, predicate) INCLUDE (dst, weight)")
    op.execute(f"CREATE INDEX semantic_edge_rev_idx ON {s}semantic_edge (dst, predicate) INCLUDE (src, weight)")
    op.execute(f"CREATE INDEX semantic_edge_bank_idx ON {s}semantic_edge (bank_id, predicate)")

    # -- Bundle registry --------------------------------------------------------
    op.execute(f"""
        CREATE TABLE {s}okf_bundle (
            bundle_id    uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            bank_id      text        NOT NULL,
            okf_version  text        NOT NULL DEFAULT '0.2',
            built_at     timestamptz NOT NULL DEFAULT now(),
            concept_count int        NOT NULL,
            digest       bytea       NOT NULL,
            manifest     jsonb       NOT NULL,
            redaction_policy text    NOT NULL DEFAULT 'default'
        )
    """)
    op.execute(f"""
        CREATE TABLE {s}okf_bundle_log (
            bank_id   text        NOT NULL,
            dir_path  text        NOT NULL,
            logged_on date        NOT NULL,
            kind      text        NOT NULL,
            concept_path text     NOT NULL,
            detail    text,
            PRIMARY KEY (bank_id, dir_path, logged_on, concept_path, kind)
        )
    """)

    # -- Coalescing distill queue (§2.5) ----------------------------------------
    op.execute(f"""
        CREATE TABLE {s}okf_dirty (
            bank_id      text        NOT NULL,
            subject_kind text        NOT NULL,
            subject_id   text        NOT NULL,
            dirty_since  timestamptz NOT NULL DEFAULT now(),
            reason       integer     NOT NULL DEFAULT 0,
            PRIMARY KEY (bank_id, subject_kind, subject_id),
            CONSTRAINT okf_dirty_subject_kind_nonempty CHECK (length(btrim(subject_kind)) > 0)
        )
    """)
    op.execute(f"CREATE INDEX okf_dirty_claim_idx ON {s}okf_dirty (bank_id, dirty_since)")

    # -- Human-gated computation proposals (D9 output) ---------------------------
    op.execute(f"""
        CREATE TABLE {s}okf_computation_proposal (
            proposal_id   uuid PRIMARY KEY DEFAULT gen_random_uuid(),
            bank_id       text        NOT NULL,
            target_path   text        NOT NULL,
            runtime       text        NOT NULL,
            parameters    jsonb       NOT NULL DEFAULT '[]'::jsonb,
            computation_path   text,
            computation_inline text,
            executor_resource text,
            attester_resource text,
            source_pattern text       NOT NULL,
            evidence      jsonb       NOT NULL DEFAULT '{{}}'::jsonb,
            proposed_by   text        NOT NULL,
            proposed_at   timestamptz NOT NULL DEFAULT now(),
            status        text        NOT NULL DEFAULT 'pending'
                          CHECK (status IN ('pending','promoted','rejected')),
            reviewed_by   text,
            reviewed_at   timestamptz,
            CONSTRAINT okf_proposal_one_form CHECK (
                (computation_path IS NULL) <> (computation_inline IS NULL)
            ),
            CONSTRAINT okf_proposal_human_promotion CHECK (
                status <> 'promoted' OR reviewed_by LIKE 'human:%'
            )
        )
    """)
    op.execute(f"CREATE INDEX okf_proposal_list_idx ON {s}okf_computation_proposal (bank_id, status)")

    # -- I4 provenance closure (constraint trigger) ------------------------------
    op.execute(f"""
        CREATE FUNCTION {s}okf_assert_grounded() RETURNS trigger
        LANGUAGE plpgsql AS $fn$
        DECLARE n int;
        BEGIN
            -- Concept deleted again within the same transaction: nothing
            -- survives to police (M0-found spurious-violation guard).
            IF NOT EXISTS (SELECT 1 FROM {s}okf_concept c WHERE c.concept_id = NEW.concept_id) THEN
                RETURN NEW;
            END IF;
            IF NEW.generated_by LIKE 'human:%' THEN
                RETURN NEW;
            END IF;
            SELECT count(*) INTO n
              FROM {s}okf_source src
             WHERE src.concept_id = NEW.concept_id
               AND src.memory_id IS NOT NULL;
            IF n = 0 THEN
                RAISE EXCEPTION
                  'I4 violation: machine-generated concept % (%) has no grounding memory',
                  NEW.path, NEW.concept_id
                  USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END $fn$
    """)
    op.execute(f"""
        CREATE CONSTRAINT TRIGGER okf_concept_grounded
            AFTER INSERT OR UPDATE OF generated_by ON {s}okf_concept
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION {s}okf_assert_grounded()
    """)

    # -- I7 no LLM-authored execution (constraint trigger) -----------------------
    op.execute(f"""
        CREATE FUNCTION {s}okf_assert_authorship() RETURNS trigger
        LANGUAGE plpgsql AS $fn$
        BEGIN
            IF NOT EXISTS (SELECT 1 FROM {s}okf_concept c WHERE c.concept_id = NEW.concept_id) THEN
                RETURN NEW;
            END IF;
            IF NEW.status = 'stable'
               AND NOT EXISTS (SELECT 1 FROM {s}okf_verified v
                                WHERE v.concept_id = NEW.concept_id
                                  AND (v.actor LIKE 'human:%' OR v.actor LIKE 'process:%'))
            THEN
                RAISE EXCEPTION
                  'I7 violation: promotion of % to stable requires a human: or process: verification',
                  NEW.path USING ERRCODE = 'integrity_constraint_violation';
            END IF;
            RETURN NEW;
        END $fn$
    """)
    op.execute(f"""
        CREATE CONSTRAINT TRIGGER okf_concept_authorship
            AFTER INSERT OR UPDATE OF status ON {s}okf_concept
            DEFERRABLE INITIALLY DEFERRED
            FOR EACH ROW EXECUTE FUNCTION {s}okf_assert_authorship()
    """)

    # -- W9 guard: OKF-originated sessions may not enqueue dirty marks ------------
    # Sessions belonging to the OKF subsystem set `okf.suppress_dirty = on`;
    # any dirty write attempted in that context is a recursion bug and fails
    # loudly here (okf_recursive_dirty_total must remain 0).
    op.execute(f"""
        CREATE FUNCTION {s}okf_dirty_suppress_guard() RETURNS trigger
        LANGUAGE plpgsql AS $fn$
        BEGIN
            IF current_setting('okf.suppress_dirty', true) = 'on' THEN
                RAISE EXCEPTION
                  'W9 violation: dirty mark on %.% enqueued from OKF subsystem context',
                  NEW.subject_kind, NEW.subject_id
                  USING ERRCODE = 'raise_exception';
            END IF;
            RETURN NEW;
        END $fn$
    """)
    op.execute(f"""
        CREATE TRIGGER okf_dirty_suppress_guard
            BEFORE INSERT OR UPDATE ON {s}okf_dirty
            FOR EACH ROW EXECUTE FUNCTION {s}okf_dirty_suppress_guard()
    """)


def _pg_downgrade() -> None:
    s = _get_schema_prefix()
    op.execute(f"DROP TRIGGER IF EXISTS okf_dirty_suppress_guard ON {s}okf_dirty")
    op.execute(f"DROP TRIGGER IF EXISTS okf_concept_authorship ON {s}okf_concept")
    op.execute(f"DROP TRIGGER IF EXISTS okf_concept_grounded ON {s}okf_concept")
    op.execute(f"DROP FUNCTION IF EXISTS {s}okf_dirty_suppress_guard()")
    op.execute(f"DROP FUNCTION IF EXISTS {s}okf_assert_authorship()")
    op.execute(f"DROP FUNCTION IF EXISTS {s}okf_assert_grounded()")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_computation_proposal")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_dirty")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_bundle_log")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_bundle")
    op.execute(f"DROP TABLE IF EXISTS {s}semantic_edge")
    op.execute(f"DROP TABLE IF EXISTS {s}semantic_node")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_concept_section")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_computation")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_source")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_verified")
    op.execute(f"DROP TABLE IF EXISTS {s}okf_concept")
    op.execute(f"DROP TYPE IF EXISTS {s}semantic_node_kind")
    op.execute(f"DROP TYPE IF EXISTS {s}okf_distill_class")
    op.execute(f"DROP TYPE IF EXISTS {s}okf_status")


def upgrade() -> None:
    run_for_dialect(pg=_pg_upgrade)


def downgrade() -> None:
    run_for_dialect(pg=_pg_downgrade)
