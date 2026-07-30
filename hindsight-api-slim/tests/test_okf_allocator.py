"""Unit tests for the M7 budget allocator and query planner (pure logic)."""

import pytest

from hindsight_api.okf.allocator import TierCandidates, dedupe_against_concepts, water_fill
from hindsight_api.okf.planner import plan


def _items(n: int, body_len: int = 400) -> list[dict]:
    return [{"body": "x" * body_len, "id": f"i{k}"} for k in range(n)]


class TestWaterFill:
    def test_alpha_caps_respected(self):
        out = water_fill(
            [TierCandidates(1, _items(10, 400)), TierCandidates(3, _items(10, 400))],
            max_tokens=200,
        )
        # α1 = 0.30 → 60 tokens for tier 1 (600 chars each → 150 tokens per item)
        assert out["spent"][1] <= 60 + 1e-9

    def test_spillover_flows_down(self):
        out = water_fill(
            [TierCandidates(1, []), TierCandidates(3, _items(20, 400))],
            max_tokens=200,
        )
        # No tier-1 items → tier 3 gets α1's budget back (full 200 minus its own items' fit)
        assert out["spent"][3] > 200 * 0.40

    def test_truncation_marks_and_stops(self):
        out = water_fill([TierCandidates(1, _items(5, 400))], max_tokens=100)
        kept = out["tiers"][1]
        assert kept[-1].get("truncated") is True
        assert len(kept) < 5

    def test_alpha_must_sum_to_one(self):
        with pytest.raises(ValueError):
            water_fill([TierCandidates(1, [])], max_tokens=100, alpha=(0.5, 0.5, 0.5, 0.5))

    def test_residual_returned(self):
        out = water_fill([TierCandidates(1, [])], max_tokens=100)
        assert out["residual"] > 0


class TestDedupe:
    def test_cited_memories_dropped(self):
        memories = [{"id": "m1"}, {"id": "m2"}, {"id": "m3"}]
        concepts = [{"sources": [{"memory_id": "m2"}]}]
        assert [m["id"] for m in dedupe_against_concepts(memories, concepts)] == ["m1", "m3"]

    def test_no_sources_no_change(self):
        memories = [{"id": "m1"}]
        assert dedupe_against_concepts(memories, [{"sources": []}]) == memories

    def test_type_coercion(self):
        memories = [{"id": 123}]
        concepts = [{"sources": [{"memory_id": "123"}]}]
        assert dedupe_against_concepts(memories, concepts) == []


class TestPlanner:
    def test_pure_deterministic(self):
        assert plan("Project Zephyr", "auto") == plan("Project Zephyr", "auto")

    def test_entity_seed_for_short_names(self):
        p = plan("Project Zephyr", "auto")
        assert p["entity_seeds"] == ["Project Zephyr"]
        assert p["graph_assist"] is True

    def test_no_seed_for_sentences(self):
        p = plan("what do we know about the zephyr project?", "auto")
        assert p["entity_seeds"] == []

    def test_tier_order(self):
        p = plan("anything", "auto")
        assert p["tiers"] == [0, 1, 2, 3]
        p2 = plan("anything", "memories_only")
        assert p2["tiers"] == [3]

    def test_temporal_detection(self):
        assert plan("what happened last week", "auto")["temporal_constraint"] is True
        assert plan("Project Zephyr", "auto")["temporal_constraint"] is False
