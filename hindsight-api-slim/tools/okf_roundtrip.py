#!/usr/bin/env python3
"""Export → import → export digest round-trip (I5 acceptance, G3).

The bundle digest is sha256 over sorted (path, content_hash), so equality
after a round-trip is a real structural check: every concept's (path, type,
title, description, body) survived intact. Status/generated_by legitimately
change (imports pin to draft under a human: actor) and are not hashed.

Usage: okf_roundtrip.py [source_bank] [scratch_bank]
Exit 0 = digests equal, 1 = mismatch.
"""

import json
import sys
import urllib.request

BASE = "http://127.0.0.1:8888/v1/default/banks"
SRC = sys.argv[1] if len(sys.argv) > 1 else "okf-test"
DST = sys.argv[2] if len(sys.argv) > 2 else "okf-roundtrip"
ACTOR = "human:roundtrip-check"


def call(method: str, url: str, body=None, raw: bool = False):
    data = None
    headers = {}
    if body is not None and not isinstance(body, bytes):
        data = json.dumps(body).encode()
        headers["Content-Type"] = "application/json"
    elif isinstance(body, bytes):
        data = body
        headers["Content-Type"] = "application/gzip"
    req = urllib.request.Request(url, data=data, headers=headers, method=method)
    with urllib.request.urlopen(req, timeout=60) as resp:
        payload = resp.read()
        return payload if raw else json.loads(payload)


def main() -> int:
    # 1. Ensure the scratch bank exists.
    call("PUT", f"{BASE}/{DST}", {"name": DST})
    # 2. Export source → tarball.
    built = call("POST", f"{BASE}/{SRC}/okf/bundles")
    digest_a = built["digest"]
    tarball = call("GET", f"{BASE}/{SRC}/okf/bundles/{built['bundle_id']}", raw=True)
    print(f"exported {SRC}: {built['concept_count']} concepts, digest {digest_a[:16]}…")
    # 3. Import into scratch bank.
    result = call("POST", f"{BASE}/{DST}/okf/bundles/import-tarball?imported_by={ACTOR}", body=tarball)
    print(f"imported into {DST}: {result}")
    # 4. Re-export scratch bank.
    rebuilt = call("POST", f"{BASE}/{DST}/okf/bundles")
    digest_b = rebuilt["digest"]
    print(f"re-exported {DST}: {rebuilt['concept_count']} concepts, digest {digest_b[:16]}…")
    # 5. Compare.
    ok = digest_a == digest_b
    print("ROUND-TRIP:", "PASS — digests identical" if ok else f"FAIL — {digest_a[:16]}… != {digest_b[:16]}…")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
