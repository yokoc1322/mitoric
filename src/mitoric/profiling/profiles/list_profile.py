"""List column profiling."""

from __future__ import annotations

import polars as pl

from mitoric.models.aggregation import Histogram, ListLengthStats, ListProfile
from mitoric.profiling.histograms.builder import build_numeric_histograms
from mitoric.profiling.utils.sampling import collect_sample_values


def build_list_profile(values: pl.Series) -> ListProfile:
    series = values.drop_nulls()
    list_values = (
        series.arr.to_list() if str(series.dtype).startswith("Array") else series
    )
    lengths = list_values.list.len()

    length_stats = _build_length_stats(lengths)
    length_histograms = _build_length_histograms(lengths)
    value_samples = collect_sample_values(list_values)

    return ListProfile(
        length_stats=length_stats,
        length_histograms=length_histograms,
        value_samples=value_samples,
    )


def _build_length_stats(lengths: pl.Series) -> ListLengthStats:
    if lengths.is_empty():
        return ListLengthStats(mean=0.0, median=0.0, minimum=0, maximum=0)

    mean_value = lengths.mean()
    median_value = lengths.median()
    min_value = lengths.min()
    max_value = lengths.max()
    return ListLengthStats(
        mean=_require_float_value(mean_value),
        median=_require_float_value(median_value),
        minimum=_require_int_value(min_value),
        maximum=_require_int_value(max_value),
    )


def _build_length_histograms(lengths: pl.Series) -> list[Histogram]:
    length_values = lengths.cast(pl.Float64)
    return build_numeric_histograms(length_values, is_integer=True)


def _require_float_value(value: object | None) -> float:
    if value is None:
        raise ValueError("value is required")
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float)):
        return float(value)
    raise TypeError("value must be numeric")


def _require_int_value(value: object | None) -> int:
    if value is None:
        raise ValueError("value is required")
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    if isinstance(value, float):
        return int(value)
    raise TypeError("value must be integer")
