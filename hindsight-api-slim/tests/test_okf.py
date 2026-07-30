"""Unit tests for the OKF Phase-4 package (pure logic, no database fixtures).

Covers the deterministic seams that everything else depends on: slugify,
section splitting, content hashing, computation binding + attestation, and
bundle scaffold rendering.
"""

from datetime import date, datetime, timezone

import pytest

from hindsight_api.okf.compute import ComputeError, _bind, _normalize_sql, attest_computation
from hindsight_api.okf.exporter import _render_index, _render_log
from hindsight_api.okf.projectors import _content_hash, _split_sections, slugify


class TestSlugify:
    def test_basic(self):
        assert slugify("Project Zephyr") == "project-zephyr"

    def test_punctuation_collapses(self):
        assert slugify("  Maria  Chen (Lead)!! ") == "maria-chen-lead"

    def test_empty_falls_back(self):
        assert slugify("!!!") == "unnamed"


class TestSplitSections:
    def test_heading_boundaries(self):
        body = "intro line\n# Facts\n- a\n- b\n# Related\n* x\n"
        parts = _split_sections(body)
        headings = [h for h, _ in parts]
        assert headings == [None, "# Facts", "# Related"]
        assert parts[1][1].startswith("# Facts")

    def test_no_headings_single_chunk(self):
        parts = _split_sections("just prose\nmore prose\n")
        assert len(parts) == 1
        assert parts[0][0] is None

    def test_empty_body(self):
        assert _split_sections("") == []


class TestContentHash:
    def test_deterministic(self):
        assert _content_hash("a", "b") == _content_hash("a", "b")

    def test_sensitive_to_any_part(self):
        assert _content_hash("a", "b") != _content_hash("a", "c")
        assert _content_hash("a", "b") != _content_hash("a", "b", "c")

    def test_length_is_sha256(self):
        assert len(_content_hash("x")) == 32


class TestBind:
    def test_binds_in_first_appearance_order(self):
        sql, args = _bind("SELECT * FROM t WHERE a = @x AND b = @y AND c = @x", {"x": 1, "y": 2})
        assert sql == "SELECT * FROM t WHERE a = $1 AND b = $2 AND c = $1"
        assert args == [1, 2]

    def test_missing_parameter_raises(self):
        with pytest.raises(ComputeError):
            _bind("SELECT @needed", {})

    def test_no_placeholders(self):
        sql, args = _bind("SELECT 1", {"unused": 5})
        assert sql == "SELECT 1" and args == []


class TestNormalizeSql:
    def test_collapses_whitespace_and_case(self):
        assert _normalize_sql("SELECT\n  a,\n    b  FROM   t;") == "select a, b from t"


class TestAttestComputation:
    INLINE = "SELECT count(*) FROM okf_concept WHERE bank_id = @bank"

    def test_pass_on_exact_receipt(self):
        receipt = {"executed_sql": "SELECT count(*) FROM okf_concept WHERE bank_id = $1"}
        verdict = attest_computation(receipt=receipt, computation_inline=self.INLINE, parameters={"bank": "b1"})
        assert verdict["verdict"] == "pass"
        assert verdict["expected_sql"] == receipt["executed_sql"]

    def test_fail_on_tampered_receipt(self):
        receipt = {"executed_sql": "SELECT count(*) FROM okf_concept"}
        verdict = attest_computation(receipt=receipt, computation_inline=self.INLINE, parameters={"bank": "b1"})
        assert verdict["verdict"] == "fail"

    def test_whitespace_insensitive(self):
        receipt = {"executed_sql": "SELECT  count(*) FROM okf_concept WHERE bank_id = $1;"}
        verdict = attest_computation(receipt=receipt, computation_inline=self.INLINE, parameters={"bank": "b1"})
        assert verdict["verdict"] == "pass"


class TestRenderScaffold:
    def _concept(self, path, title, description=None, created=None, updated=None, status="stable"):
        created = created or datetime(2026, 7, 30, tzinfo=timezone.utc)
        return {
            "path": path,
            "title": title,
            "description": description,
            "created_at": created,
            "updated_at": updated or created,
            "status": status,
        }

    def test_root_index_carries_only_okf_version_frontmatter(self):
        out = _render_index("", [self._concept("entities/x", "X")], is_root=True)
        assert out.startswith('---\nokf_version: "0.2"\n---')
        assert "type:" not in out.split("---")[1]

    def test_log_groups_newest_first(self):
        older = self._concept("a/b", "B", created=datetime(2026, 7, 1, tzinfo=timezone.utc))
        newer = self._concept("a/c", "C", created=datetime(2026, 7, 30, tzinfo=timezone.utc))
        out = _render_log([older, newer])
        assert out.index("## 2026-07-30") < out.index("## 2026-07-01")
        assert "**Creation**" in out

    def test_log_marks_deprecation(self):
        deprecated = self._concept("a/d", "D", status="deprecated")
        assert "**Deprecation**" in _render_log([deprecated])
