from __future__ import annotations

import datetime as dt

import polars as pl

from mitoric.profiling.profiles.datetime import build_datetime_profile


def test_build_datetime_profile_top_values() -> None:
    values = pl.Series(
        "values",
        [
            dt.date(2024, 1, 1),
            dt.date(2024, 1, 2),
            dt.date(2024, 1, 2),
            dt.date(2024, 1, 3),
        ],
    )

    profile = build_datetime_profile(values)

    assert profile.min_datetime == "2024-01-01"
    assert profile.max_datetime == "2024-01-03"
    assert profile.histograms
    assert profile.top_values[0].value == "2024-01-02"
    assert profile.top_values[0].count == 2
