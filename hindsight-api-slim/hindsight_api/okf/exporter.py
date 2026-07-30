"""Bundle export (spec §3.10, M3): materialize a bank's OKF concepts into a
conformant OKF v0.2 bundle — one markdown file per concept with YAML
frontmatter, per-directory ``index.md``, a root ``log.md`` — plus a
content-addressable digest, and pack it as a gzipped tarball.
"""

from __future__ import annotations

import hashlib
import io
import json
import logging
import tarfile
from collections import defaultdict

import yaml

from ..engine.db_utils import acquire_with_retry
from ..engine.schema import fq_table

logger = logging.getLogger(__name__)


def _frontmatter(concept: dict, sources: list[dict], verified: list[dict]) -> dict:
    fm: dict = {"type": concept["type"]}
    if concept["title"]:
        fm["title"] = concept["title"]
    if concept["description"]:
        fm["description"] = concept["description"]
    if concept["resource"]:
        fm["resource"] = concept["resource"]
    if concept["tags"]:
        fm["tags"] = list(concept["tags"])
    fm["status"] = concept["status"]
    if concept["stale_after"]:
        fm["stale_after"] = concept["stale_after"].isoformat()
    fm["generated"] = {"by": concept["generated_by"], "at": concept["generated_at"].isoformat()}
    if verified:
        fm["verified"] = [{"by": v["actor"], "at": v["verified_at"].isoformat()} for v in verified]
    if sources:
        fm["sources"] = [
            {
                k: v
                for k, v in {
                    "id": s["source_key"],
                    "resource": s["resource"],
                    "title": s["title"],
                    "author": s["author"],
                    "usage_count": s["usage_count"],
                    "last_modified": s["last_modified"].isoformat() if s["last_modified"] else None,
                }.items()
                if v is not None
            }
            for s in sources
        ]
    ext = concept["extensions"] or {}
    if isinstance(ext, str):
        ext = json.loads(ext)
    # Producer-defined extension keys round-trip verbatim [OKF §4.1].
    fm.update({k: v for k, v in ext.items() if v is not None})
    return fm


def render_concept_md(concept: dict, sources: list[dict], verified: list[dict]) -> str:
    fm = _frontmatter(concept, sources, verified)
    return f"---\n{yaml.safe_dump(fm, sort_keys=False, allow_unicode=True)}---\n\n{concept['body']}"


def _render_index(dir_path: str, items: list[dict], is_root: bool) -> str:
    lines = []
    if is_root:
        # The only frontmatter permitted in an index.md: okf_version [OKF §12].
        lines.extend(["---", 'okf_version: "0.2"', "---", ""])
    lines.extend(["# Index", ""])
    for c in items:
        fname = c["path"].rsplit("/", 1)[-1] + ".md"
        desc = f" - {c['description']}" if c["description"] else ""
        lines.append(f"* [{c['title'] or c['path']}]({fname}){desc}")
    # Subdirectories get an entry too (progressive disclosure [OKF §8]).
    for sub in sorted({c["path"].rsplit("/", 1)[0] for c in items if "/" in c["path"]}):
        rel = sub[len(dir_path):].lstrip("/") if dir_path else sub
        if rel and "/" not in rel:
            lines.append(f"* [{rel}/]({rel}/)")
    return "\n".join(lines) + "\n"


def _render_log(concepts: list[dict]) -> str:
    events: dict = defaultdict(list)
    for c in concepts:
        events[c["created_at"].date()].append(("Creation", c))
        if c["updated_at"].date() > c["created_at"].date():
            events[c["updated_at"].date()].append(("Update", c))
        if c["status"] == "deprecated":
            events[c["updated_at"].date()].append(("Deprecation", c))
    lines = ["# Directory Update Log", ""]
    for day in sorted(events, reverse=True):
        lines.append(f"## {day.isoformat()}")
        for kind, c in sorted(events[day], key=lambda e: e[1]["path"]):
            lines.append(f"* **{kind}**: [{c['title'] or c['path']}](/{c['path']}.md)")
        lines.append("")
    return "\n".join(lines) + "\n"


async def build_bundle(backend, *, bank_id: str, redaction_policy: str = "default", record: bool = True) -> dict:
    """Build the bank's bundle. When ``record`` is set, writes an okf_bundle row
    (manifest + digest). Returns dict with bundle metadata + tarball bytes."""
    concepts_t = fq_table("okf_concept")
    sources_t = fq_table("okf_source")
    verified_t = fq_table("okf_verified")

    async with acquire_with_retry(backend) as conn:
        concepts = [dict(r) for r in await conn.fetch(
            f"SELECT * FROM {concepts_t} WHERE bank_id = $1 ORDER BY path", bank_id
        )]
        if not concepts:
            return {"bundle_id": None, "digest": None, "concept_count": 0, "tarball": None, "manifest": None}
        ids = [c["concept_id"] for c in concepts]
        src_rows = await conn.fetch(f"SELECT * FROM {sources_t} WHERE concept_id = ANY($1::uuid[])", ids)
        ver_rows = await conn.fetch(f"SELECT * FROM {verified_t} WHERE concept_id = ANY($1::uuid[])", ids)

        src_by: dict = defaultdict(list)
        for s in src_rows:
            src_by[s["concept_id"]].append(dict(s))
        ver_by: dict = defaultdict(list)
        for v in ver_rows:
            ver_by[v["concept_id"]].append(dict(v))

        files: dict[str, str] = {}
        for c in concepts:
            files[f"{c['path']}.md"] = render_concept_md(c, src_by[c["concept_id"]], ver_by[c["concept_id"]])

        dirs: dict = defaultdict(list)
        for c in concepts:
            parent = c["path"].rsplit("/", 1)[0] if "/" in c["path"] else ""
            dirs[parent].append(c)
        for dir_path, items in sorted(dirs.items()):
            prefix = f"{dir_path}/" if dir_path else ""
            files[f"{prefix}index.md"] = _render_index(dir_path, items, is_root=not dir_path)
        if "" not in dirs:
            # All concepts live in subdirectories — still emit a root index
            # (progressive disclosure [OKF §8]; only place okf_version is allowed).
            top = sorted({c["path"].split("/", 1)[0] for c in concepts})
            lines = ["---", 'okf_version: "0.2"', "---", "", "# Index", ""]
            lines.extend(f"* [{d}/]({d}/)" for d in top)
            files["index.md"] = "\n".join(lines) + "\n"
        files["log.md"] = _render_log(concepts)

        digest = hashlib.sha256()
        for c in concepts:
            digest.update(c["path"].encode("utf-8"))
            digest.update(b"\x00")
            digest.update(c["content_hash"])

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w:gz") as tf:
            for relpath in sorted(files):
                data = files[relpath].encode("utf-8")
                info = tarfile.TarInfo(relpath)
                info.size = len(data)
                tf.addfile(info, io.BytesIO(data))
        tarball = buf.getvalue()

        manifest = {
            "bank_id": bank_id,
            "concept_count": len(concepts),
            "files": sorted(files),
            "okf_version": "0.2",
            "redaction_policy": redaction_policy,
        }
        bundle_id = None
        built_at = None
        if record:
            row = await conn.fetchrow(
                f"""INSERT INTO {fq_table('okf_bundle')}
                        (bank_id, okf_version, concept_count, digest, manifest, redaction_policy)
                    VALUES ($1, '0.2', $2, $3, $4::jsonb, $5)
                    RETURNING bundle_id, built_at""",
                bank_id,
                len(concepts),
                digest.digest(),
                json.dumps(manifest),
                redaction_policy,
            )
            bundle_id = str(row["bundle_id"])
            built_at = row["built_at"].isoformat() if row["built_at"] else None

            # Populate okf_bundle_log (§3.10) — the bundle history is durable
            # state, not just materialized at export time.
            log_t = fq_table("okf_bundle_log")
            for c in concepts:
                entries = [("Creation", c["created_at"].date())]
                if c["updated_at"].date() > c["created_at"].date():
                    entries.append(("Update", c["updated_at"].date()))
                if c["status"] == "deprecated":
                    entries.append(("Deprecation", c["updated_at"].date()))
                dir_path = c["path"].rsplit("/", 1)[0] if "/" in c["path"] else ""
                for kind, day in entries:
                    await conn.execute(
                        f"""INSERT INTO {log_t} (bank_id, dir_path, logged_on, kind, concept_path, detail)
                            VALUES ($1, $2, $3, $4, $5, $6)
                            ON CONFLICT (bank_id, dir_path, logged_on, concept_path, kind) DO NOTHING""",
                        bank_id,
                        dir_path,
                        day,
                        kind,
                        c["path"],
                        (c["description"] or "")[:200],
                    )
            logger.info(f"okf bundle exported for bank_id={bank_id}: {bundle_id} ({len(concepts)} concepts, {len(files)} files)")

        return {
            "bundle_id": bundle_id,
            "digest": digest.hexdigest(),
            "concept_count": len(concepts),
            "file_count": len(files),
            "built_at": built_at,
            "tarball": tarball,
            "manifest": manifest,
        }


async def get_bundle_row(backend, *, bank_id: str, bundle_id: str) -> dict | None:
    async with acquire_with_retry(backend) as conn:
        row = await conn.fetchrow(
            f"SELECT bundle_id, bank_id, digest, concept_count, manifest, built_at FROM {fq_table('okf_bundle')} WHERE bundle_id = $1 AND bank_id = $2",
            bundle_id,
            bank_id,
        )
        return dict(row) if row else None
