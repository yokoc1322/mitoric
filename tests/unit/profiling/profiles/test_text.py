from __future__ import annotations

import polars as pl

from mitoric.profiling.profiles.text import build_text_profile


def test_build_text_profile_length_histogram() -> None:
    values = pl.Series("values", ["a", "bb", "bb", "ccc"])

    profile = build_text_profile(values)

    assert profile.length_histograms
    histogram = profile.length_histograms[0]
    assert histogram.bin_count == 3
    assert [(bin.lower, bin.upper, bin.count) for bin in histogram.bins] == [
        (1.0, 1.0, 1),
        (2.0, 2.0, 2),
        (3.0, 3.0, 1),
    ]
