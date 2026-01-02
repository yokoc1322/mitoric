from __future__ import annotations

import polars as pl

from mitoric.profiling.compare.histograms import build_compare_numeric_histograms


def test_build_compare_numeric_histograms_low_cardinality() -> None:
    left_values = pl.Series("left", [1.0, 2.0, 2.0])
    right_values = pl.Series("right", [1.0, 3.0])

    histograms = build_compare_numeric_histograms(
        left_values,
        right_values,
        is_integer=True,
    )

    assert len(histograms) == 1
    histogram = histograms[0]
    assert histogram.labels == ["1", "2", "3"]
    assert histogram.left_counts == [1, 2, 0]
    assert histogram.right_counts == [1, 0, 1]
