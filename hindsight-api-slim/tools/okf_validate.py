#!/usr/bin/env python3
"""Independent OKF v0.2 §11 conformance validator.

Deliberately has NO hindsight dependency: it consumes a bundle tarball (or a
directory) the way any OKF-aware agent would — plain files only. This is the
M3 gate's "independent OKF consumer reads bundle with no Hindsight dependency"
check, and it's what CI should run against every golden bundle.

Usage: okf_validate.py <bundle.tar.gz | directory>
Exit 0 = conformant, 1 = violations found.
"""

import re
import sys
import tarfile
from pathlib import Path

import yaml

RESERVED = {"index.md", "log.md"}
DATE_HEADING = re.compile(r"^## \d{4}-\d{2}-\d{2}$")


def split_frontmatter(text: str):
    if not text.startswith("---\n"):
        return None, text
    end = text.find("\n---", 4)
    if end == -1:
        return None, text
    return text[4:end], text[end + 4:].lstrip("\n")


def validate(files: dict[str, str]) -> list[str]:
    violations: list[str] = []
    concepts = 0
    for path, content in sorted(files.items()):
        name = Path(path).name
        fm_text, _body = split_frontmatter(content)
        if name in RESERVED:
            if name == "index.md" and fm_text is not None:
                fm = yaml.safe_load(fm_text) or {}
                # Only okf_version is permitted, and only at the bundle root [§8, §12].
                if set(fm) - {"okf_version"}:
                    violations.append(f"{path}: index.md carries non-okf_version frontmatter {sorted(fm)}")
                if "okf_version" in fm and path != "index.md":
                    violations.append(f"{path}: okf_version outside bundle-root index.md")
            if name == "log.md":
                headings = [ln for ln in content.splitlines() if ln.startswith("## ")]
                bad = [h for h in headings if not DATE_HEADING.match(h)]
                if bad:
                    violations.append(f"{path}: non-ISO date headings: {bad[:3]}")
            continue

        concepts += 1
        if fm_text is None:
            violations.append(f"{path}: concept has no frontmatter block")
            continue
        try:
            fm = yaml.safe_load(fm_text)
        except yaml.YAMLError as e:
            violations.append(f"{path}: unparseable frontmatter: {e}")
            continue
        if not isinstance(fm, dict):
            violations.append(f"{path}: frontmatter is not a mapping")
            continue
        type_ = fm.get("type")
        if not type_ or not str(type_).strip():
            violations.append(f"{path}: missing or empty required 'type'")
        verified = fm.get("verified")
        if isinstance(verified, dict):
            pass  # bare mapping MUST be read as a one-element list [§5.2] — tolerated
        sources = fm.get("sources")
        if sources is not None:
            if not isinstance(sources, list):
                violations.append(f"{path}: sources is not a list")
            else:
                for i, s in enumerate(sources):
                    if not isinstance(s, dict) or not s.get("resource"):
                        violations.append(f"{path}: sources[{i}] missing required 'resource' [§5.1]")
        status = fm.get("status")
        if status is not None and status not in ("draft", "stable", "deprecated"):
            violations.append(f"{path}: unknown status {status!r}")
        # §11 tolerances exercised implicitly: unknown types, unknown keys, and
        # broken links are all accepted without violation.
    if concepts == 0:
        violations.append("bundle contains no concept documents")
    return violations


def main() -> int:
    target = sys.argv[1]
    files: dict[str, str] = {}
    if target.endswith((".tar.gz", ".tgz")):
        with tarfile.open(target) as tf:
            for m in tf.getmembers():
                if m.isfile() and m.name.endswith(".md"):
                    files[m.name.lstrip("./")] = tf.extractfile(m).read().decode("utf-8")
    else:
        for p in sorted(Path(target).rglob("*.md")):
            files[str(p.relative_to(target))] = p.read_text()
    violations = validate(files)
    n = sum(1 for f in files if Path(f).name not in RESERVED)
    if violations:
        print(f"FAIL: {len(violations)} violation(s) across {n} concepts, {len(files)} files")
        for v in violations:
            print(f"  - {v}")
        return 1
    print(f"PASS: OKF v0.2 §11 conformant — {n} concepts, {len(files)} files, 0 violations")
    return 0


if __name__ == "__main__":
    sys.exit(main())
