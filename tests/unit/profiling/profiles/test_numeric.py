from __future__ import annotations

import polars as pl

from mitoric.profiling.profiles.numeric import build_numeric_profile


def test_build_numeric_profile_low_cardinality_histogram() -> None:
    values = pl.Series("values", [1.0, 2.0, 2.0, 3.0])

    profile = build_numeric_profile(values, is_integer=True)

    assert profile.histograms
    histogram = profile.histograms[0]
    assert histogram.bin_count == 3
    assert [(bin.lower, bin.upper, bin.count) for bin in histogram.bins] == [
        (1.0, 1.0, 1),
        (2.0, 2.0, 2),
        (3.0, 3.0, 1),
    ]
    assert profile.outlier_rate == 0.0
