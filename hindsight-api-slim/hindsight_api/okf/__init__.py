"""OKF Phase 4 / M2: concept store, dirty-marking, projection subroutines.

Deterministic (zero-LLM) distillation of Hindsight state into OKF v0.2
concepts. See plans/hindsight-integration/phase-4-okf-semantic-layer.md
(spec §2) and migration revision c3e5f7a9b1d4 for the data model.
"""

from .dirty import (
    REASON_CONCEPT_INVALIDATED,
    REASON_FACTS_ADDED,
    REASON_MENTAL_MODEL_REFRESHED,
    REASON_OBSERVATION_UPDATED,
    mark_dirty,
    submit_okf_distill,
)
from .distiller import run_okf_distill_job
from .projectors import project_entity, project_mental_model, project_observation_profile, slugify

__all__ = [
    "REASON_CONCEPT_INVALIDATED",
    "REASON_FACTS_ADDED",
    "REASON_MENTAL_MODEL_REFRESHED",
    "REASON_OBSERVATION_UPDATED",
    "mark_dirty",
    "submit_okf_distill",
    "run_okf_distill_job",
    "project_entity",
    "project_mental_model",
    "project_observation_profile",
    "slugify",
]
