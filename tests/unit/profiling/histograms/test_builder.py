from __future__ import annotations

import polars as pl

from mitoric.profiling.histograms.builder import build_numeric_histograms


def test_build_numeric_histograms_constant_values() -> None:
    values = pl.Series("values", [2.0, 2.0, 2.0])

    histograms = build_numeric_histograms(values, is_integer=True)

    for histogram in histograms:
        assert len(histogram.bins) == 1
        assert histogram.bins[0].lower == 2.0
        assert histogram.bins[0].upper == 2.0
        assert histogram.bins[0].count == 3
