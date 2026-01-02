"""Datetime column profiling."""

from __future__ import annotations

import polars as pl

from mitoric.models.aggregation import DatetimeProfile, DatetimeValueCount
from mitoric.profiling.histograms.builder import (
    build_datetime_histograms,
    normalize_datetime_values,
)
from mitoric.profiling.utils.constants import TOP_VALUES_LIMIT


def build_datetime_profile(
    values: pl.Series,
) -> DatetimeProfile:
    series = values.drop_nulls()
    if series.len() == 0:
        return DatetimeProfile(
            min_datetime="",
            max_datetime="",
            histograms=[],
            top_values=[],
        )
    normalized, numeric_values, is_time = normalize_datetime_values(series)
    normalized_sorted = normalized.sort()
    counts = normalized.value_counts().sort(
        ["count", "value"], descending=[True, False]
    )
    return DatetimeProfile(
        min_datetime=str(normalized_sorted.min()),
        max_datetime=str(normalized_sorted.max()),
        histograms=build_datetime_histograms(numeric_values, is_time=is_time),
        top_values=_top_datetime_values(counts),
    )


def _top_datetime_values(counts: pl.DataFrame) -> list[DatetimeValueCount]:
    return [
        DatetimeValueCount(value=str(row["value"]), count=int(row["count"]))
        for row in counts.head(TOP_VALUES_LIMIT).iter_rows(named=True)
    ]
