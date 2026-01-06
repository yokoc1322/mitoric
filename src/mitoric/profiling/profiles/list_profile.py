"""List column profiling."""

from __future__ import annotations

import polars as pl

from mitoric.models.aggregation import ListLengthStats, ListProfile


def build_list_profile(values: pl.Series) -> ListProfile:
    series = values.drop_nulls()
    lengths = series.list.len()

    if lengths.is_empty():
        length_stats = ListLengthStats(mean=0.0, median=0.0, minimum=0, maximum=0)
    else:
        mean_value = lengths.mean()
        median_value = lengths.median()
        min_value = lengths.min()
        max_value = lengths.max()
        length_stats = ListLengthStats(
            mean=_require_float_value(mean_value),
            median=_require_float_value(median_value),
            minimum=_require_int_value(min_value),
            maximum=_require_int_value(max_value),
        )

    return ListProfile(length_stats=length_stats)


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
