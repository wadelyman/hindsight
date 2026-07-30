"""Tier 0 — Attested Computation (spec §2.4 D9, §3.5, OKF §10; M5 core).

Flow: an agent PROPOSES a computation (recurring numeric query pattern) → a
human: reviewer PROMOTES it (database-enforced, I7 — an agent actor can never
promote) → consumers RUN it with declared parameters (read-only, receipted) →
an ATTESTER (deterministic, no LLM) confirms the executed SQL is exactly the
sanctioned computation bound with the claimed parameters.
"""

from __future__ import annotations

import json
import logging
import re
import uuid as _uuid
from datetime import UTC, datetime

from ..engine.schema import fq_table
from .projectors import PRODUCER

logger = logging.getLogger(__name__)

SUPPORTED_RUNTIMES = {"postgres"}
_PARAM_RE = re.compile(r"@([A-Za-z_][A-Za-z0-9_]*)")


class ComputeError(Exception):
    pass


def _bind(computation_sql: str, parameters: dict) -> tuple[str, list]:
    """Bind @name placeholders to positional params in first-appearance order."""
    names: list[str] = []

    def _sub(m: re.Match) -> str:
        name = m.group(1)
        if name not in parameters:
            raise ComputeError(f"missing required parameter: {name}")
        names.append(name)
        return f"${len(names)}"

    return _PARAM_RE.sub(_sub, computation_sql), [parameters[n] for n in names]


def _normalize_sql(sql: str) -> str:
    return re.sub(r"\s+", " ", sql.strip().rstrip(";")).lower()


async def submit_proposal(conn, *, bank_id: str, payload: dict, proposed_by: str | None = None) -> dict:
    """D9 output: an agent proposes an Attested Computation. Status: pending —
    nothing here is executable until a human promotes it."""
    required = ["target_path", "runtime", "source_pattern"]
    missing = [k for k in required if not payload.get(k)]
    if missing:
        raise ComputeError(f"missing required fields: {missing}")
    if payload["runtime"] not in SUPPORTED_RUNTIMES:
        raise ComputeError(f"unsupported runtime {payload['runtime']!r}; supported: {sorted(SUPPORTED_RUNTIMES)}")
    if bool(payload.get("computation_path")) == bool(payload.get("computation_inline")):
        raise ComputeError("exactly one of computation_path / computation_inline is required")

    proposal_id = _uuid.uuid4()
    await conn.execute(
        f"""INSERT INTO {fq_table('okf_computation_proposal')}
                (proposal_id, bank_id, target_path, runtime, parameters,
                 computation_path, computation_inline, executor_resource,
                 attester_resource, source_pattern, evidence, proposed_by)
            VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,$11::jsonb,$12)""",
        proposal_id,
        bank_id,
        payload["target_path"],
        payload["runtime"],
        json.dumps(payload.get("parameters") or []),
        payload.get("computation_path"),
        payload.get("computation_inline"),
        payload.get("executor_resource"),
        payload.get("attester_resource"),
        payload["source_pattern"],
        json.dumps(payload.get("evidence") or {}),
        proposed_by or PRODUCER,
    )
    return {"proposal_id": str(proposal_id), "status": "pending"}


async def promote_proposal(conn, *, bank_id: str, proposal_id: str, reviewed_by: str) -> dict:
    """Human-gated promotion (I7): requires a human: actor — enforced again at
    the database layer by okf_proposal_human_promotion CHECK."""
    if not reviewed_by.startswith("human:"):
        raise ComputeError("promotion requires a human:<id> reviewer")

    proposals_t = fq_table("okf_computation_proposal")
    concepts_t = fq_table("okf_concept")
    computations_t = fq_table("okf_computation")
    verified_t = fq_table("okf_verified")

    row = await conn.fetchrow(
        f"""UPDATE {proposals_t}
            SET status = 'promoted', reviewed_by = $3, reviewed_at = now()
            WHERE proposal_id = $1 AND bank_id = $2 AND status = 'pending'
            RETURNING *""",
        proposal_id,
        bank_id,
        reviewed_by,
    )
    if not row:
        raise ComputeError("proposal not found or already resolved")

    inline = row["computation_inline"]
    body_text = "# Computation\n\n```\n" + (inline if inline else f"see {row['computation_path']}") + "\n```\n"
    concept_id = _uuid.uuid4()
    await conn.execute(
        f"""INSERT INTO {concepts_t}
                (concept_id, bank_id, path, type, title, description, tags, status,
                 generated_by, generated_at, body, distill_class, content_hash, okf_version)
            VALUES ($1,$2,$3,'Attested Computation',$4,$5,$6,'stable',$7,now(),$8,'projection',$9,'0.2')""",
        concept_id,
        bank_id,
        row["target_path"],
        f"Computation: {row['target_path'].rsplit('/', 1)[-1]}",
        row["source_pattern"][:140],
        ["computation"],
        reviewed_by,
        body_text,
        b"\x00" * 32,
    )
    await conn.execute(
        f"""INSERT INTO {computations_t}
                (concept_id, runtime, parameters, computation_path, computation_inline,
                 executor_resource, attester_resource, authored_by)
            VALUES ($1,$2,$3::jsonb,$4,$5,$6,$7,$8)""",
        concept_id,
        row["runtime"],
        json.dumps(row["parameters"] or []),
        row["computation_path"],
        row["computation_inline"],
        row["executor_resource"],
        row["attester_resource"],
        reviewed_by,
    )
    await conn.execute(
        f"INSERT INTO {verified_t} (concept_id, actor, verified_at) VALUES ($1, $2, now())",
        concept_id,
        reviewed_by,
    )
    return {"path": row["target_path"], "concept_id": str(concept_id), "status": "stable"}


async def _load_computation(conn, *, bank_id: str, path: str) -> dict:
    row = await conn.fetchrow(
        f"""SELECT c.path, c.status, c.generated_by,
                   EXISTS (SELECT 1 FROM {fq_table('okf_verified')} v
                            WHERE v.concept_id = c.concept_id AND v.actor LIKE 'human:%') AS human_verified,
                   comp.runtime, comp.parameters, comp.computation_path, comp.computation_inline
            FROM {fq_table('okf_concept')} c
            JOIN {fq_table('okf_computation')} comp ON comp.concept_id = c.concept_id
            WHERE c.bank_id = $1 AND c.path = $2""",
        bank_id,
        path,
    )
    if not row:
        raise ComputeError("computation not found")
    result = dict(row)
    # Gate per spec §5.7: only stable + human-attested computations may run.
    if result["status"] != "stable" or not result["human_verified"]:
        raise ComputeError("computation is not runnable: requires status=stable and human attestation")
    if result["runtime"] not in SUPPORTED_RUNTIMES:
        raise ComputeError(f"unsupported runtime {result['runtime']!r}")
    if not result["computation_inline"]:
        raise ComputeError("path-form computations are not executable in this deployment")
    return result


async def run_computation(conn, *, bank_id: str, path: str, parameters: dict) -> dict:
    """Execute a sanctioned computation read-only and produce a receipt."""
    comp = await _load_computation(conn, bank_id=bank_id, path=path)
    bound_sql, bind_args = _bind(comp["computation_inline"], parameters)

    if not _normalize_sql(bound_sql).startswith("select"):
        raise ComputeError("only SELECT computations may execute")

    await conn.execute("SET LOCAL statement_timeout = '10s'")
    await conn.execute("SET TRANSACTION READ ONLY")
    rows = await conn.fetch(bound_sql, *bind_args)
    receipt = {
        "job_id": _uuid.uuid4().hex,
        "runtime": comp["runtime"],
        "executed_sql": bound_sql,
        "parameters": parameters,
        "row_count": len(rows),
        "rows": [dict(r) for r in rows[:50]],
        "truncated": len(rows) > 50,
        "executed_at": datetime.now(UTC).isoformat(),
    }
    return receipt


def attest_computation(*, receipt: dict, computation_inline: str, parameters: dict) -> dict:
    """Deterministic provenance attestation (OKF §10.2/§10.5, no LLM):
    the receipt's executed SQL must be exactly the sanctioned computation
    bound with the claimed parameters."""
    expected_sql, _ = _bind(computation_inline, parameters)
    match = _normalize_sql(expected_sql) == _normalize_sql(receipt.get("executed_sql", ""))
    return {
        "verdict": "pass" if match else "fail",
        "expected_sql": expected_sql,
        "received_sql": receipt.get("executed_sql"),
        "attester": "sql-equality/v1",
    }
