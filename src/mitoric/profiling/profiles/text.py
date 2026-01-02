"""Text column profiling."""

from __future__ import annotations

import polars as pl

from mitoric.models.aggregation import TextLengthStats, TextProfile, TokenCount
from mitoric.profiling.histograms.builder import build_numeric_histograms
from mitoric.profiling.utils.constants import TOP_VALUES_LIMIT


def build_text_profile(values: pl.Series) -> TextProfile:
    series = values.drop_nulls()
    if series.name != "value":
        series = series.rename("value")

    lengths = series.str.len_chars()
    if lengths.len() > 0:
        mean_value = lengths.mean()
        median_value = lengths.median()
        min_value = lengths.min()
        max_value = lengths.max()
        length_stats = TextLengthStats(
            mean=_require_float_value(mean_value),
            median=_require_float_value(median_value),
            minimum=_require_int_value(min_value),
            maximum=_require_int_value(max_value),
        )
    else:
        length_stats = TextLengthStats(mean=0.0, median=0.0, minimum=0, maximum=0)
    counts = series.value_counts().sort(["count", "value"], descending=[True, False])
    top_tokens = [
        TokenCount(token=str(row["value"]), count=int(row["count"]))
        for row in counts.head(TOP_VALUES_LIMIT).iter_rows(named=True)
    ]
    length_histograms = build_numeric_histograms(
        lengths.cast(pl.Float64), is_integer=True
    )
    return TextProfile(
        length_stats=length_stats,
        top_tokens=top_tokens,
        length_histograms=length_histograms,
    )


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
