from __future__ import annotations

import polars as pl

from mitoric.profiling.profiles.categorical import build_categorical_profile


def test_build_categorical_profile_high_cardinality() -> None:
    values = pl.Series("values", [f"cat_{idx}" for idx in range(120)])

    profile = build_categorical_profile(values, unique_count=120)

    assert profile.is_high_cardinality is True
    assert len(profile.top_categories) == 10
    assert profile.suppressed_count == 110
    assert profile.histograms
