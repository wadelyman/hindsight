"""Bounded RPQ traversal over the unified semantic graph (spec §3.8–3.9, M4).

Spreading activation A(n_j) = max over in-edges of A(n_i) · w · δ · ψ(φ) · υ(n_j),
evaluated hop-by-hop with hard caps: ``max_hops`` (bounded repetition — an
unbounded Kleene star over a memory graph is an availability incident), a
``max_nodes_examined`` budget, and a statement timeout. The existing memory
graph (``memory_links``) is unioned in place, never copied.
"""

from __future__ import annotations

import logging

from ..engine.schema import fq_table
from .graph import PROVENANCE_WEIGHTS

logger = logging.getLogger(__name__)

DEFAULT_MAX_HOPS = 3
DEFAULT_DECAY = 0.35  # HINDSIGHT_API_SEMANTIC_DECAY; boot-assert δ·μ·ψ·υ < 1
DEFAULT_NODE_BUDGET = 50_000
DEFAULT_STATEMENT_TIMEOUT_MS = 250
MIN_ACTIVATION = 0.01


def _semantic_cfg() -> dict:
    try:
        from ..config import get_config

        cfg = get_config()
        return {
            "max_hops": getattr(cfg, "semantic_max_hops", DEFAULT_MAX_HOPS),
            "decay": getattr(cfg, "semantic_decay", DEFAULT_DECAY),
            "max_nodes": getattr(cfg, "semantic_max_nodes", DEFAULT_NODE_BUDGET),
            "timeout": getattr(cfg, "semantic_statement_timeout", DEFAULT_STATEMENT_TIMEOUT_MS),
        }
    except Exception:
        return {
            "max_hops": DEFAULT_MAX_HOPS,
            "decay": DEFAULT_DECAY,
            "max_nodes": DEFAULT_NODE_BUDGET,
            "timeout": DEFAULT_STATEMENT_TIMEOUT_MS,
        }


def _trust_factor(concept_row: dict | None) -> float:
    """υ(n) — trust damping (spec §3.9). Applies to concept nodes only."""
    if concept_row is None:
        return 1.0
    if concept_row["status"] == "deprecated":
        return 0.0
    tier = 1.35 if concept_row["human_verified"] else (1.15 if concept_row["machine_verified"] else 1.00)
    stale = 0.6 if concept_row["is_stale"] else 1.0
    return tier * stale


async def traverse(
    conn,
    *,
    bank_id: str,
    seed_node_ids: list[int],
    max_hops: int | None = None,
    decay: float | None = None,
    max_nodes_examined: int | None = None,
    predicate_filter: list[str] | None = None,
    statement_timeout_ms: int | None = None,
    direction: str = "forward",
) -> dict:
    """Bounded spreading-activation traversal from a seed set.

    ``direction`` is "forward" (edges OUT of the frontier) or "reverse"
    (edges INTO the frontier — e.g. concept → its grounding memories via
    okf:source_of, entity → concepts describing it). Caps default to the
    HINDSIGHT_API_SEMANTIC_* config values.

    Returns {nodes: [{node_id, kind, ref_id, activation, hops, parent}],
             truncated: bool, nodes_examined: int}.
    """
    cfg = _semantic_cfg()
    max_hops = max_hops if max_hops is not None else cfg["max_hops"]
    decay = decay if decay is not None else cfg["decay"]
    max_nodes_examined = max_nodes_examined if max_nodes_examined is not None else cfg["max_nodes"]
    statement_timeout_ms = statement_timeout_ms if statement_timeout_ms is not None else cfg["timeout"]
    edges_t = fq_table("semantic_edge")
    links_t = fq_table("memory_links")
    nodes_t = fq_table("semantic_node")
    concepts_t = fq_table("okf_concept")
    verified_t = fq_table("okf_verified")

    await conn.execute(f"SET LOCAL statement_timeout = '{int(statement_timeout_ms)}ms'")

    frontier: dict[int, dict] = {}
    visited: dict[int, dict] = {}
    seed_rows = await conn.fetch(
        f"SELECT node_id, kind, ref_id FROM {nodes_t} WHERE node_id = ANY($1::bigint[])",
        seed_node_ids,
    )
    for r in seed_rows:
        frontier[r["node_id"]] = {
            "node_id": r["node_id"],
            "kind": r["kind"],
            "ref_id": r["ref_id"],
            "activation": 1.0,
            "hops": 0,
            "parent": None,
        }
    visited.update(frontier)

    nodes_examined = 0
    truncated = False

    for hop in range(1, max_hops + 1):
        if not frontier or nodes_examined >= max_nodes_examined:
            truncated = truncated or bool(frontier)
            break

        frontier_ids = list(frontier)
        mem_ref_ids = [n["ref_id"] for n in frontier.values() if n["kind"] == "memory"]

        # Normalized to (frontier_node_id, candidate_node_id, predicate, weight, provenance)
        edge_rows: list[tuple] = []
        if direction == "forward":
            if predicate_filter:
                rows = await conn.fetch(
                    f"SELECT src, dst, predicate, weight, provenance FROM {edges_t} WHERE src = ANY($1::bigint[]) AND predicate = ANY($2::text[])",
                    frontier_ids,
                    predicate_filter,
                )
            else:
                rows = await conn.fetch(
                    f"SELECT src, dst, predicate, weight, provenance FROM {edges_t} WHERE src = ANY($1::bigint[])",
                    frontier_ids,
                )
            edge_rows += [(r["src"], r["dst"], r["predicate"], r["weight"], r["provenance"]) for r in rows]
            if mem_ref_ids and not predicate_filter:
                rows = await conn.fetch(
                    f"""SELECT sn_src.node_id AS src, sn_dst.node_id AS dst,
                               ml.link_type AS predicate, 1.0::real AS weight, 'extracted' AS provenance
                        FROM {links_t} ml
                        JOIN {nodes_t} sn_src ON sn_src.kind = 'memory' AND sn_src.ref_id = ml.from_unit_id
                        JOIN {nodes_t} sn_dst ON sn_dst.kind = 'memory' AND sn_dst.ref_id = ml.to_unit_id
                        WHERE ml.from_unit_id = ANY($1::uuid[])""",
                    mem_ref_ids,
                )
                edge_rows += [(r["src"], r["dst"], r["predicate"], r["weight"], r["provenance"]) for r in rows]
        else:
            if predicate_filter:
                rows = await conn.fetch(
                    f"SELECT src, dst, predicate, weight, provenance FROM {edges_t} WHERE dst = ANY($1::bigint[]) AND predicate = ANY($2::text[])",
                    frontier_ids,
                    predicate_filter,
                )
            else:
                rows = await conn.fetch(
                    f"SELECT src, dst, predicate, weight, provenance FROM {edges_t} WHERE dst = ANY($1::bigint[])",
                    frontier_ids,
                )
            edge_rows += [(r["dst"], r["src"], r["predicate"], r["weight"], r["provenance"]) for r in rows]
            if mem_ref_ids and not predicate_filter:
                rows = await conn.fetch(
                    f"""SELECT sn_src.node_id AS src, sn_dst.node_id AS dst,
                               ml.link_type AS predicate, 1.0::real AS weight, 'extracted' AS provenance
                        FROM {links_t} ml
                        JOIN {nodes_t} sn_src ON sn_src.kind = 'memory' AND sn_src.ref_id = ml.from_unit_id
                        JOIN {nodes_t} sn_dst ON sn_dst.kind = 'memory' AND sn_dst.ref_id = ml.to_unit_id
                        WHERE ml.to_unit_id = ANY($1::uuid[])""",
                    mem_ref_ids,
                )
                edge_rows += [(r["dst"], r["src"], r["predicate"], r["weight"], r["provenance"]) for r in rows]
        nodes_examined += len(edge_rows)

        candidate_ids = list({cand for (_front, cand, _p, _w, _prov) in edge_rows} - set(visited))
        if not candidate_ids:
            break
        node_rows = await conn.fetch(
            f"SELECT node_id, kind, ref_id FROM {nodes_t} WHERE node_id = ANY($1::bigint[])",
            candidate_ids,
        )
        concept_meta: dict = {}
        concept_ref_ids = [r["ref_id"] for r in node_rows if r["kind"] == "concept"]
        if concept_ref_ids:
            for c in await conn.fetch(
                f"""SELECT c.concept_id, c.status,
                          (c.stale_after IS NOT NULL AND c.stale_after <= CURRENT_DATE) AS is_stale,
                          EXISTS (SELECT 1 FROM {verified_t} v WHERE v.concept_id = c.concept_id AND v.actor LIKE 'human:%') AS human_verified,
                          EXISTS (SELECT 1 FROM {verified_t} v WHERE v.concept_id = c.concept_id AND v.actor LIKE 'process:%') AS machine_verified
                   FROM {concepts_t} c WHERE c.concept_id = ANY($1::uuid[])""",
                concept_ref_ids,
            ):
                concept_meta[c["concept_id"]] = dict(c)

        node_by_id = {r["node_id"]: r for r in node_rows}
        next_frontier: dict[int, dict] = {}
        for front_id, cand_id, predicate, weight, provenance in edge_rows:
            dst_id = cand_id
            if dst_id in visited or dst_id not in node_by_id:
                continue
            dst = node_by_id[dst_id]
            src_act = frontier[front_id]["activation"]
            psi = PROVENANCE_WEIGHTS.get(provenance, 1.0)
            upsilon = _trust_factor(concept_meta.get(dst["ref_id"])) if dst["kind"] == "concept" else 1.0
            act = src_act * weight * decay * psi * upsilon
            if act < MIN_ACTIVATION or act <= 0:
                continue
            prev = next_frontier.get(dst_id)
            if prev is None or act > prev["activation"]:
                next_frontier[dst_id] = {
                    "node_id": dst_id,
                    "kind": dst["kind"],
                    "ref_id": dst["ref_id"],
                    "activation": act,
                    "hops": hop,
                    "parent": front_id,
                }

        visited.update(next_frontier)
        frontier = next_frontier

    from .metrics import inc, observe

    max_hop_reached = max((n["hops"] for n in visited.values()), default=0)
    observe("semantic_query_hops", float(max_hop_reached), bank=bank_id)
    observe("semantic_nodes_examined", float(nodes_examined), bank=bank_id)
    if truncated:
        inc("semantic_truncated_total", 1, bank=bank_id)

    return {
        "nodes": sorted(visited.values(), key=lambda n: -n["activation"]),
        "truncated": truncated,
        "nodes_examined": nodes_examined,
    }
