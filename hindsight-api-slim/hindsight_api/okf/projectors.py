"""Projection subroutines (spec §2.4): deterministic, zero-LLM concept rewrites.

Each projector rewrites ONE subject's concept(s) from current database state
and is idempotent: identical state yields an identical ``content_hash`` and is
skipped (§6.11 ``okf_content_hash_skip_total``). All writes carry grounding
``okf_source`` rows (I4) and a ``process:okf-distill`` verification (machine-
confirmed trust tier, which is what allows ``status: stable`` under I7).
"""

from __future__ import annotations

import hashlib
import logging
import re
from datetime import UTC, date, datetime, timedelta

from ..engine.schema import fq_table

logger = logging.getLogger(__name__)

PRODUCER = "hindsight/0.8.1+okf"  # agent actor [OKF §7]
PROCESS_VERIFIER = "process:okf-distill"  # machine-confirmed tier [OKF §5.3]
STALE_AFTER_DAYS = 30  # OKF_TTL_DEFAULT (spec §5.2)
FACT_SAMPLE_LIMIT = 12
RELATED_LIMIT = 8


def slugify(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    return slug or "unnamed"


def _content_hash(*parts: str) -> bytes:
    h = hashlib.sha256()
    for part in parts:
        h.update(part.encode("utf-8"))
        h.update(b"\x00")
    return h.digest()


async def _upsert_concept(
    conn,
    *,
    bank_id: str,
    path: str,
    type_: str,
    title: str,
    description: str | None,
    resource: str | None,
    tags: list[str],
    body: str,
    sources: list[dict],
    extensions: dict | None = None,
) -> dict:
    """Idempotent concept upsert + source/verification rewrite (one transaction,
    the caller's). Machine-generated ⇒ grounding sources are mandatory (I4)."""
    concepts = fq_table("okf_concept")
    src_table = fq_table("okf_source")
    ver_table = fq_table("okf_verified")

    grounded = [s for s in sources if s.get("memory_id")]
    if not grounded:
        return {"path": path, "skipped": "no_grounding"}

    import json as _json

    new_hash = _content_hash(type_, title, description or "", body)
    existing = await conn.fetchrow(
        f"SELECT concept_id, content_hash FROM {concepts} WHERE bank_id = $1 AND path = $2",
        bank_id,
        path,
    )
    if existing and existing["content_hash"] == new_hash:
        return {"path": path, "skipped": "unchanged"}

    stale_after = date.today() + timedelta(days=STALE_AFTER_DAYS)
    row = await conn.fetchrow(
        f"""INSERT INTO {concepts}
                (bank_id, path, type, title, description, resource, tags, status,
                 stale_after, generated_by, generated_at, extensions, body,
                 distill_class, content_hash, source_generation, okf_version)
            VALUES ($1,$2,$3,$4,$5,$6,$7,'stable',$8,$9,$10,$11::jsonb,$12,'projection',$13,1,'0.2')
            ON CONFLICT (bank_id, path) DO UPDATE SET
                title = EXCLUDED.title,
                description = EXCLUDED.description,
                resource = EXCLUDED.resource,
                tags = EXCLUDED.tags,
                status = EXCLUDED.status,
                stale_after = EXCLUDED.stale_after,
                generated_by = EXCLUDED.generated_by,
                generated_at = EXCLUDED.generated_at,
                extensions = EXCLUDED.extensions,
                body = EXCLUDED.body,
                content_hash = EXCLUDED.content_hash,
                source_generation = {concepts}.source_generation + 1,
                updated_at = now()
            RETURNING concept_id""",
        bank_id,
        path,
        type_,
        title,
        description,
        resource,
        tags,
        stale_after,
        PRODUCER,
        datetime.now(UTC),
        _json.dumps(extensions or {}),
        body,
        new_hash,
    )
    concept_id = row["concept_id"]

    await conn.execute(f"DELETE FROM {src_table} WHERE concept_id = $1", concept_id)
    for s in sources:
        await conn.execute(
            f"""INSERT INTO {src_table}
                    (concept_id, source_key, resource, title, author, usage_count,
                     last_modified, usage_from, usage_to, memory_id)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)""",
            concept_id,
            s["source_key"],
            s["resource"],
            s.get("title"),
            s.get("author"),
            s.get("usage_count"),
            s.get("last_modified"),
            s.get("usage_from"),
            s.get("usage_to"),
            s.get("memory_id"),
        )

    # Machine-confirmed verification (I7: stable requires human:/process: actor).
    await conn.execute(f"DELETE FROM {ver_table} WHERE concept_id = $1 AND actor = $2", concept_id, PROCESS_VERIFIER)
    await conn.execute(
        f"INSERT INTO {ver_table} (concept_id, actor, verified_at) VALUES ($1, $2, now())",
        concept_id,
        PROCESS_VERIFIER,
    )

    # Section chunks (§3.7): heading-boundary splits so retrieval can inject a
    # section rather than a whole document. token_count is a len/4 estimate.
    await _write_sections(conn, concept_id=concept_id, body=body)
    return {"path": path, "concept_id": str(concept_id), "sources": len(sources)}


def _split_sections(body: str) -> list[tuple[str | None, str]]:
    parts: list[tuple[str | None, list[str]]] = []
    current_heading: str | None = None
    current: list[str] = []
    for line in body.splitlines():
        if line.startswith("# ") or line.startswith("## "):
            if current or current_heading is not None:
                parts.append((current_heading, current))
            current_heading = line.strip()
            current = [line]
        else:
            current.append(line)
    if current or current_heading is not None:
        parts.append((current_heading, current))
    return [(h, "\n".join(lines).strip()) for h, lines in parts if "\n".join(lines).strip()]


async def _write_sections(conn, *, concept_id, body: str) -> None:
    sections = fq_table("okf_concept_section")
    await conn.execute(f"DELETE FROM {sections} WHERE concept_id = $1", concept_id)
    for ordinal, (heading, chunk_body) in enumerate(_split_sections(body)):
        await conn.execute(
            f"INSERT INTO {sections} (concept_id, ordinal, heading, body, token_count) VALUES ($1, $2, $3, $4, $5)",
            concept_id,
            ordinal,
            heading,
            chunk_body,
            len(chunk_body) // 4,
        )


async def project_entity(conn, *, bank_id: str, entity_id: str) -> dict | None:
    """D3 project_entity_index: canonical entity + facts + co-mentioned entities
    → ``Entity`` concept at ``entities/{slug}`` (spec §2.4, §3.4)."""
    import uuid as _uuid

    entity_id = _uuid.UUID(str(entity_id))
    entities = fq_table("entities")
    units = fq_table("memory_units")
    unit_entities = fq_table("unit_entities")

    ent = await conn.fetchrow(
        f"SELECT id, canonical_name, mention_count, first_seen, last_seen FROM {entities} WHERE id = $1 AND bank_id = $2",
        entity_id,
        bank_id,
    )
    if not ent:
        return None

    facts = await conn.fetch(
        f"""SELECT u.id, u.text, u.created_at
            FROM {units} u
            JOIN {unit_entities} ue ON ue.unit_id = u.id
            WHERE ue.entity_id = $1 AND u.bank_id = $2
            ORDER BY u.created_at DESC
            LIMIT $3""",
        entity_id,
        bank_id,
        FACT_SAMPLE_LIMIT,
    )
    if not facts:
        return None

    related = await conn.fetch(
        f"""SELECT e2.canonical_name, count(*) AS shared
            FROM {unit_entities} a
            JOIN {unit_entities} b ON a.unit_id = b.unit_id AND b.entity_id <> $1
            JOIN {entities} e2 ON e2.id = b.entity_id
            WHERE a.entity_id = $1
            GROUP BY e2.canonical_name
            ORDER BY shared DESC, e2.canonical_name
            LIMIT $2""",
        entity_id,
        RELATED_LIMIT,
    )

    name = ent["canonical_name"]
    slug = slugify(name)
    path = f"entities/{slug}"

    sources: list[dict] = []
    fact_lines: list[str] = []
    footnotes: list[str] = []
    for i, f in enumerate(facts):
        key = f"mem-{i + 1}"
        text = (f["text"] or "").strip()
        created = f["created_at"]
        sources.append(
            {
                "source_key": key,
                "resource": f"hindsight://{bank_id}/memory/{f['id']}",
                "title": text[:80],
                "author": PRODUCER,
                "last_modified": created.date() if isinstance(created, datetime) else None,
                "memory_id": f["id"],
            }
        )
        fact_lines.append(f"- {text}[^{key}]")
        footnotes.append(f"[^{key}]: {text[:140]}")

    related_lines = [
        f"* [{r['canonical_name']}](/entities/{slugify(r['canonical_name'])}.md) — {r['shared']} shared fact(s)"
        for r in related
    ] or ["* none yet *"]

    body = "\n".join(
        [
            f"{name} is an entity tracked in this memory bank.",
            "",
            "# Facts",
            *fact_lines,
            "",
            "# Related",
            *related_lines,
            "",
            *footnotes,
            "",
        ]
    )

    description = facts[0]["text"][:140] if facts else None
    return await _upsert_concept(
        conn,
        bank_id=bank_id,
        path=path,
        type_="Entity",
        title=name,
        description=description,
        resource=f"hindsight://{bank_id}/entity/{entity_id}",
        tags=["entity", slug],
        body=body,
        sources=sources,
        extensions={"hindsight_mention_count": ent["mention_count"]},
    )


async def project_observation_profile(conn, *, bank_id: str, entity_id: str) -> dict | None:
    """D2 project_observation: entity's observation facts → ``Entity Profile``
    concept at ``entities/{slug}/profile``. Returns None when the entity has no
    observation facts yet (consolidation hasn't synthesized any).

    Observation facts (`fact_type='observation'`) carry no unit_entities rows;
    they link to entities through ``source_memory_ids`` — the raw facts they
    were synthesized from."""
    import uuid as _uuid

    entity_id = _uuid.UUID(str(entity_id))
    entities = fq_table("entities")
    units = fq_table("memory_units")
    unit_entities = fq_table("unit_entities")

    ent = await conn.fetchrow(
        f"SELECT id, canonical_name FROM {entities} WHERE id = $1 AND bank_id = $2",
        entity_id,
        bank_id,
    )
    if not ent:
        return None

    observations = await conn.fetch(
        f"""SELECT DISTINCT u.id, u.text, u.created_at
            FROM {units} u
            JOIN {unit_entities} ue ON ue.unit_id = ANY(u.source_memory_ids)
            WHERE ue.entity_id = $1 AND u.bank_id = $2 AND u.fact_type = 'observation'
            ORDER BY u.created_at DESC
            LIMIT $3""",
        entity_id,
        bank_id,
        FACT_SAMPLE_LIMIT,
    )
    if not observations:
        return None

    name = ent["canonical_name"]
    slug = slugify(name)
    path = f"entities/{slug}/profile"

    sources: list[dict] = []
    lines: list[str] = []
    footnotes: list[str] = []
    for i, f in enumerate(observations):
        key = f"obs-{i + 1}"
        text = (f["text"] or "").strip()
        created = f["created_at"]
        sources.append(
            {
                "source_key": key,
                "resource": f"hindsight://{bank_id}/memory/{f['id']}",
                "title": text[:80],
                "author": PRODUCER,
                "last_modified": created.date() if isinstance(created, datetime) else None,
                "memory_id": f["id"],
            }
        )
        lines.append(f"- {text}[^{key}]")
        footnotes.append(f"[^{key}]: {text[:140]}")

    body = "\n".join(
        [
            f"Preference-neutral profile of {name}, synthesized from observation facts.",
            "",
            "# Profile",
            *lines,
            "",
            *footnotes,
            "",
        ]
    )

    return await _upsert_concept(
        conn,
        bank_id=bank_id,
        path=path,
        type_="Entity Profile",
        title=f"{name} — Profile",
        description=(observations[0]["text"] or "")[:140],
        resource=f"hindsight://{bank_id}/entity/{entity_id}",
        tags=["entity", slug, "profile"],
        body=body,
        sources=sources,
    )


async def project_mental_model(conn, *, bank_id: str, mental_model_id: str) -> dict | None:
    """D1 project_mental_model: mental model + reflect provenance →
    ``Mental Model`` concept at ``mental-models/{slug}`` (spec §2.4, §3.10)."""
    models = fq_table("mental_models")

    mm = await conn.fetchrow(
        f"""SELECT id, name, content, source_query, tags, description,
                   entity_id, last_refreshed_at, reflect_response
            FROM {models} WHERE id = $1 AND bank_id = $2""",
        str(mental_model_id),
        bank_id,
    )
    if not mm or not mm["content"]:
        return None

    rr = mm["reflect_response"] or {}
    if isinstance(rr, str):
        # asyncpg returns jsonb as text when no codec is registered
        import json as _json

        rr = _json.loads(rr)
    based_on = rr.get("based_on") or {}
    # based_on is keyed by fact network (world/experience/observation/...);
    # only memory-network entries are memory units (I4 grounding).
    rr_memories: list = []
    for network, entries in based_on.items():
        if network in ("mental-models", "mental_models", "directives"):
            continue
        if isinstance(entries, list):
            rr_memories.extend(entries)

    sources: list[dict] = []
    grounding_lines: list[str] = []
    for i, m in enumerate(rr_memories[:FACT_SAMPLE_LIMIT]):
        if not isinstance(m, dict):
            continue
        mem_id = m.get("id") or m.get("memory_id")
        text = (m.get("text") or m.get("content") or "").strip()
        if not mem_id:
            continue
        key = f"mem-{len(sources) + 1}"
        sources.append(
            {
                "source_key": key,
                "resource": f"hindsight://{bank_id}/memory/{mem_id}",
                "title": text[:80] or None,
                "author": PRODUCER,
                "memory_id": mem_id,
            }
        )
        grounding_lines.append(f"- [^{key}]: {text[:140]}")

    if not sources:
        # I4: a mental model whose reflect provenance is unavailable must not
        # project — the concept could not be grounded.
        return None

    name = mm["name"] or f"mental-model-{mm['id']}"
    slug = slugify(name)
    path = f"mental-models/{slug}"

    body = "\n".join(
        [
            mm["content"].strip(),
            "",
            "# Source Query",
            "",
            (mm["source_query"] or "").strip() or "*not recorded*",
            "",
            "# Grounding",
            *grounding_lines,
            "",
        ]
    )

    tags = ["mental-model"]
    if isinstance(mm["tags"], list):
        tags.extend(str(t) for t in mm["tags"] if t)

    return await _upsert_concept(
        conn,
        bank_id=bank_id,
        path=path,
        type_="Mental Model",
        title=name,
        description=mm["description"] or (mm["content"] or "")[:140],
        resource=f"hindsight://{bank_id}/mental-model/{mm['id']}",
        tags=tags,
        body=body,
        sources=sources,
        extensions={
            "hindsight_bank": bank_id,
            "hindsight_source_query": mm["source_query"] or "",
            "hindsight_last_refreshed_at": mm["last_refreshed_at"].isoformat() if mm["last_refreshed_at"] else None,
        },
    )
