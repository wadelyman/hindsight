"""Semantic graph materialization (spec §3.8, M4).

Populates the ``semantic_node`` registry and ``semantic_edge`` store from the
OKF concept store. The existing memory graph (``memory_links``) is NOT copied —
the traversal engine unions it in place at query time. Idempotent: the
concept-derived predicate classes are rewritten wholesale for the bank on each
run (they are deterministic projections of current concept state).

Predicates materialized here (all φ = derived, except okf:link = linked):
- ``okf:source_of``  memory → concept   (from okf_source.memory_id; grounding)
- ``okf:contains``   concept → concept  (directory parent → child)
- ``okf:describes``  concept → entity   (resource hindsight://bank/entity/uuid)
- ``okf:link``       concept → concept  (literal markdown links in bodies)
"""

from __future__ import annotations

import logging
import re
import uuid as _uuid

from ..engine.schema import fq_table

logger = logging.getLogger(__name__)

_MD_LINK_RE = re.compile(r"\[[^\]]*\]\((/[^)\s]+?\.md)\)")
_ENTITY_RESOURCE_RE = re.compile(r"^hindsight://[^/]+/entity/([0-9a-f-]{36})$")

# ψ(φ) provenance confidence weights (spec §3.9).
PROVENANCE_WEIGHTS = {
    "asserted": 1.25,
    "linked": 1.20,
    "derived": 1.10,
    "extracted": 1.00,
    "inferred": 0.85,
}


async def _node_id(conn, nodes_t: str, bank_id: str, kind: str, ref_id) -> int:
    row = await conn.fetchrow(
        f"""INSERT INTO {nodes_t} (bank_id, kind, ref_id) VALUES ($1, $2, $3)
            ON CONFLICT (bank_id, kind, ref_id) DO UPDATE SET kind = EXCLUDED.kind
            RETURNING node_id""",
        bank_id,
        kind,
        ref_id,
    )
    return row["node_id"]


async def materialize_semantic_graph(conn, *, bank_id: str) -> dict:
    """Rewrite the bank's concept-derived semantic edges from current state.
    Returns counts per predicate. Caller provides the transaction."""
    concepts_t = fq_table("okf_concept")
    sources_t = fq_table("okf_source")
    nodes_t = fq_table("semantic_node")
    edges_t = fq_table("semantic_edge")

    concepts = await conn.fetch(
        f"SELECT concept_id, path, type, resource, body FROM {concepts_t} WHERE bank_id = $1",
        bank_id,
    )
    by_path = {c["path"]: c for c in concepts}
    node_for_concept: dict = {}

    # Registry rows first (edges need FKs).
    for c in concepts:
        node_for_concept[c["concept_id"]] = await _node_id(conn, nodes_t, bank_id, "concept", c["concept_id"])

    # Deterministic rewrite of concept-derived predicates for this bank.
    await conn.execute(
        f"DELETE FROM {edges_t} WHERE bank_id = $1 AND predicate IN ('okf:source_of','okf:contains','okf:describes','okf:link')",
        bank_id,
    )

    counts = {"okf:source_of": 0, "okf:contains": 0, "okf:describes": 0, "okf:link": 0}

    async def _edge(src: int, dst: int, predicate: str, weight: float, provenance: str) -> None:
        await conn.execute(
            f"""INSERT INTO {edges_t} (bank_id, src, dst, predicate, weight, provenance)
                VALUES ($1, $2, $3, $4, $5, $6)
                ON CONFLICT (src, dst, predicate) DO UPDATE SET weight = EXCLUDED.weight, provenance = EXCLUDED.provenance""",
            bank_id,
            src,
            dst,
            predicate,
            weight,
            provenance,
        )
        counts[predicate] += 1

    # okf:source_of — memory grounding edges (the most important predicate, §3.8).
    grounding = await conn.fetch(
        f"""SELECT s.concept_id, s.memory_id
            FROM {sources_t} s
            JOIN {concepts_t} c ON c.concept_id = s.concept_id
            WHERE c.bank_id = $1 AND s.memory_id IS NOT NULL""",
        bank_id,
    )
    for row in grounding:
        mem_node = await _node_id(conn, nodes_t, bank_id, "memory", row["memory_id"])
        await _edge(mem_node, node_for_concept[row["concept_id"]], "okf:source_of", 1.0, "derived")

    for c in concepts:
        src_node = node_for_concept[c["concept_id"]]

        # okf:contains — directory parent → child.
        if "/" in c["path"]:
            parent_path = c["path"].rsplit("/", 1)[0]
            parent = by_path.get(parent_path)
            if parent:
                await _edge(node_for_concept[parent["concept_id"]], src_node, "okf:contains", 1.0, "derived")

        # okf:describes — concept about a canonical entity.
        m = _ENTITY_RESOURCE_RE.match(c["resource"] or "")
        if m:
            ent_node = await _node_id(conn, nodes_t, bank_id, "entity", _uuid.UUID(m.group(1)))
            await _edge(src_node, ent_node, "okf:describes", 1.0, "derived")

        # okf:link — literal markdown links in the body [OKF §6.1].
        for target in _MD_LINK_RE.findall(c["body"] or ""):
            target_path = target.lstrip("/")[: -len(".md")]
            tc = by_path.get(target_path)
            if tc:
                await _edge(src_node, node_for_concept[tc["concept_id"]], "okf:link", 1.0, "linked")

    logger.info(f"semantic graph materialized for bank_id={bank_id}: {counts}")
    return counts
