"""OKF bundle import (spec §5.3, M5).

Imports pin to ``status: draft``, preserve the original ``generated.by`` in
extension keys, and are written under a ``human:`` importing actor — the I4
provenance rule exempts human-authored concepts, which is exactly right for a
cross-system import: the foreign grounding memories are not resolvable here,
so a human vouches for the transfer. Machine re-import of the same content
would be I4-rejected by design.
"""

from __future__ import annotations

import logging

import yaml

from ..engine.schema import fq_table
from .projectors import _content_hash

logger = logging.getLogger(__name__)


async def import_concepts(conn, *, bank_id: str, concepts: list[dict], imported_by: str, imported_from: str | None) -> dict:
    if not imported_by.startswith("human:"):
        raise ValueError("imported_by must be a human:<id> actor")

    concepts_t = fq_table("okf_concept")
    imported = 0
    skipped = 0
    for c in concepts:
        if not (c.get("path") and c.get("type") and c.get("body")):
            skipped += 1
            continue
        extensions = dict(c.get("extensions") or {})
        if c.get("generated_by"):
            extensions["hindsight_original_generated_by"] = c["generated_by"]
        if imported_from:
            extensions["hindsight_imported_from"] = imported_from
        import json as _json

        await conn.execute(
            f"""INSERT INTO {concepts_t}
                    (bank_id, path, type, title, description, resource, tags, status,
                     generated_by, extensions, body, distill_class, content_hash, okf_version)
                VALUES ($1,$2,$3,$4,$5,$6,$7,'draft',$8,$9::jsonb,$10,'projection',$11,'0.2')
                ON CONFLICT (bank_id, path) DO NOTHING""",
            bank_id,
            c["path"],
            c["type"],
            c.get("title"),
            c.get("description"),
            c.get("resource"),
            list(c.get("tags") or []),
            imported_by,
            _json.dumps(extensions),
            c["body"],
            _content_hash(c["type"], c.get("title") or "", c.get("description") or "", c["body"]),
        )
        imported += 1

    logger.info(f"okf import for bank_id={bank_id}: imported={imported} skipped={skipped} by={imported_by}")
    return {"imported": imported, "skipped": skipped, "status": "draft"}


MAX_TARBALL_MEMBERS = 10_000
MAX_TARBALL_BYTES = 256 * 1024 * 1024


async def import_bundle_tarball(conn, *, bank_id: str, blob: bytes, imported_by: str, imported_from: str | None = None) -> dict:
    """Ingest an OKF v0.2 bundle tarball (G3).

    Security (baseline §6.10 S2/S3) — this reads a foreign archive:
    * reject absolute paths, ``..`` segments, symlinks, and hardlinks
    * cap member count and uncompressed size (zip-bomb guard)
    * skip reserved names (``index.md``, ``log.md`` are derived, never
      concepts [OKF §3.1] — and the path CHECK forbids them anyway)
    * never execute anything under ``references/`` — imported attesters stay
      disabled pending per-digest allowlisting (S3)
    * pin every concept to ``status: draft`` — a foreign producer's 'stable'
      is not binding here
    """
    import io
    import tarfile

    concepts: list[dict] = []
    total = 0
    with tarfile.open(fileobj=io.BytesIO(blob), mode="r:gz") as tf:
        for i, m in enumerate(tf):
            if i >= MAX_TARBALL_MEMBERS:
                raise ValueError("bundle exceeds member cap")
            if not m.isfile() or m.issym() or m.islnk():
                continue
            name = m.name.lstrip("./")
            if name.startswith("/") or ".." in name.split("/"):
                raise ValueError(f"unsafe path in bundle: {m.name!r}")
            if not name.endswith(".md"):
                continue
            base = name.rsplit("/", 1)[-1]
            if base in ("index.md", "log.md"):
                continue
            total += m.size
            if total > MAX_TARBALL_BYTES:
                raise ValueError("bundle exceeds size cap")
            raw = tf.extractfile(m).read().decode("utf-8")
            parsed = _parse_concept_md(name[:-3], raw)
            if parsed:
                concepts.append(parsed)

    return await import_concepts(
        conn, bank_id=bank_id, concepts=concepts, imported_by=imported_by, imported_from=imported_from
    )


def _parse_concept_md(path: str, raw: str) -> dict | None:
    """Parse one bundle concept file (YAML frontmatter + markdown body)."""
    if not raw.startswith("---\n"):
        return None
    end = raw.find("\n---", 4)
    if end == -1:
        return None
    try:
        fm = yaml.safe_load(raw[4:end]) or {}
    except yaml.YAMLError:
        return None
    if not isinstance(fm, dict) or not fm.get("type"):
        return None
    body = raw[end + 4 :].lstrip("\n")
    reserved = {"type", "title", "description", "resource", "tags", "status", "stale_after", "generated", "verified", "sources", "usage_window"}
    return {
        "path": path,
        "type": fm["type"],
        "title": fm.get("title"),
        "description": fm.get("description"),
        "resource": fm.get("resource"),
        "tags": fm.get("tags") or [],
        "body": body,
        "generated_by": (fm.get("generated") or {}).get("by") if isinstance(fm.get("generated"), dict) else None,
        "extensions": {k: v for k, v in fm.items() if k not in reserved},
    }
