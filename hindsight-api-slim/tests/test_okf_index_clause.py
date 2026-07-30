"""C1 regression guard: index clause contract for all four vector backends.

Needs no database and no extension — which is what makes it runnable on every
matrix leg, and what would have caught the original C1 defect (a hardcoded
hnsw clause silently wrong on three of four supported backends).
"""

import pytest

from hindsight_api._vector_index import index_type_keyword, index_using_clause


@pytest.mark.parametrize(
    "ext,method",
    [
        ("pgvector", "hnsw"),
        ("vchord", "vchordrq"),
        ("pgvectorscale", "diskann"),
        ("scann", "scann"),
    ],
)
def test_okf_index_clause_matches_backend(ext, method):
    clause = index_using_clause(ext)
    assert method in clause, f"{ext} must build a {method} index, got: {clause}"
    assert index_type_keyword(ext) in clause


def test_invalid_extension_rejected():
    with pytest.raises(ValueError):
        index_using_clause("not-a-backend")
