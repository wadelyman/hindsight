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
