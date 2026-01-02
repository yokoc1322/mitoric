"""Numeric column profiling."""

from __future__ import annotations

import math
from decimal import Decimal

import polars as pl

from mitoric.models.aggregation import (
    NumericProfile,
    NumericStats,
    NumericValueCount,
    QuantileValue,
)
from mitoric.models.base import OutlierRate
from mitoric.profiling.histograms.builder import build_numeric_histograms
from mitoric.profiling.utils.constants import EXTREMES_LIMIT, TOP_VALUES_LIMIT


def build_numeric_profile(values: pl.Series, *, is_integer: bool) -> NumericProfile:
    series = values.drop_nulls()
    stats = _numeric_stats(series)
    outlier_rate = _outlier_rate(
        series,
        stats.quantiles[0].value if stats.quantiles else 0.0,
        stats.quantiles[-1].value if stats.quantiles else 0.0,
        stats.iqr,
    )
    return NumericProfile(
        is_integer=is_integer,
        stats=stats,
        outlier_rate=OutlierRate(outlier_rate),
        histograms=build_numeric_histograms(series, is_integer=is_integer),
        top_values=_top_numeric_values(series),
        min_values=_extreme_numeric_values(series, reverse=False),
        max_values=_extreme_numeric_values(series, reverse=True),
    )


def _numeric_stats(values: pl.Series) -> NumericStats:
    if values.len() == 0:
        return NumericStats(
            minimum=0.0,
            maximum=0.0,
            mean=0.0,
            median=0.0,
            std=0.0,
            variance=0.0,
            quantiles=[],
            iqr=0.0,
        )

    mean = _require_float_value(values.mean())
    median = _require_float_value(values.median())
    variance = _require_float_value(values.var(ddof=0))
    std = math.sqrt(variance)
    q1 = _require_float_value(values.quantile(0.25, interpolation="lower"))
    q3 = _require_float_value(values.quantile(0.75, interpolation="lower"))
    iqr = q3 - q1
    quantiles = [
        QuantileValue(quantile=0.25, value=q1),
        QuantileValue(quantile=0.5, value=median),
        QuantileValue(quantile=0.75, value=q3),
    ]
    return NumericStats(
        minimum=_require_float_value(values.min()),
        maximum=_require_float_value(values.max()),
        mean=mean,
        median=median,
        std=std,
        variance=variance,
        quantiles=quantiles,
        iqr=iqr,
    )


def _outlier_rate(values: pl.Series, q1: float, q3: float, iqr: float) -> float:
    if values.len() == 0 or iqr == 0.0:
        return 0.0
    lower = q1 - 1.5 * iqr
    upper = q3 + 1.5 * iqr
    outliers = ((values < lower) | (values > upper)).sum()
    return int(outliers) / values.len()


def _top_numeric_values(values: pl.Series) -> list[NumericValueCount]:
    series = values.rename("value") if values.name != "value" else values
    counts = series.value_counts().sort(["count", "value"], descending=[True, False])
    return [
        NumericValueCount(value=float(row["value"]), count=int(row["count"]))
        for row in counts.head(TOP_VALUES_LIMIT).iter_rows(named=True)
    ]


def _extreme_numeric_values(
    values: pl.Series, reverse: bool
) -> list[NumericValueCount]:
    series = values.rename("value") if values.name != "value" else values
    counts = series.value_counts().sort("value", descending=reverse)
    return [
        NumericValueCount(value=float(row["value"]), count=int(row["count"]))
        for row in counts.head(EXTREMES_LIMIT).iter_rows(named=True)
    ]


def _require_float_value(value: object | None) -> float:
    if value is None:
        raise ValueError("value is required")
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    raise TypeError("value must be numeric")
