"""Histogram utilities."""

from __future__ import annotations

import datetime as dt
import math
from decimal import Decimal

import polars as pl

from mitoric.models.aggregation import Histogram, HistogramBin, LabeledHistogram
from mitoric.profiling.histograms.config import HISTOGRAM_BINS
from mitoric.profiling.utils.constants import TOP_VALUES_LIMIT


def build_numeric_histograms(values: pl.Series, *, is_integer: bool) -> list[Histogram]:
    histograms: list[Histogram] = []
    series = values.drop_nulls()
    if series.len() == 0:
        for bin_count in HISTOGRAM_BINS:
            histograms.append(Histogram(bin_count=bin_count, bins=[]))
        return histograms

    if series.name != "value":
        series = series.rename("value")
    counts = series.value_counts().sort("value")
    unique_count = counts.height
    if unique_count <= TOP_VALUES_LIMIT:
        bins = [
            HistogramBin(
                lower=_require_float_value(row["value"]),
                upper=_require_float_value(row["value"]),
                count=int(row["count"]),
            )
            for row in counts.iter_rows(named=True)
        ]
        return [Histogram(bin_count=unique_count, bins=bins)]

    min_value = _require_float_value(series.min())
    max_value = _require_float_value(series.max())
    span = max_value - min_value
    for bin_count in HISTOGRAM_BINS:
        if span == 0:
            bins = [
                HistogramBin(
                    lower=min_value,
                    upper=min_value,
                    count=series.len(),
                )
            ]
            histograms.append(Histogram(bin_count=bin_count, bins=bins))
            continue
        if is_integer:
            integer_min = int(min_value)
            integer_max = int(max_value)
            range_size = integer_max - integer_min + 1
            width = max(1, math.ceil(range_size / bin_count))
            edges = [integer_min + width * i for i in range(bin_count + 1)]
            df = pl.DataFrame({"value": series})
            counts_df = (
                df.with_columns(
                    ((pl.col("value").cast(pl.Int64) - integer_min) // width)
                    .clip(lower_bound=0, upper_bound=bin_count - 1)
                    .alias("bin")
                )
                .group_by("bin")
                .len()
                .sort("bin")
            )
            counts_by_bin = {
                int(row["bin"]): int(row["len"])
                for row in counts_df.iter_rows(named=True)
            }
            counts = [counts_by_bin.get(index, 0) for index in range(bin_count)]
            bins = [
                HistogramBin(
                    lower=float(edges[i]),
                    upper=float(max(edges[i], min(edges[i + 1] - 1, integer_max))),
                    count=counts[i],
                )
                for i in range(bin_count)
            ]
            histograms.append(Histogram(bin_count=bin_count, bins=bins))
            continue
        width = span / bin_count
        edges = [min_value + width * i for i in range(bin_count + 1)]
        df = pl.DataFrame({"value": series})
        counts_df = (
            df.with_columns(
                pl.when(pl.col("value") == max_value)
                .then(bin_count - 1)
                .otherwise(((pl.col("value") - min_value) / width).cast(pl.Int64))
                .alias("bin")
            )
            .group_by("bin")
            .len()
            .sort("bin")
        )
        counts_by_bin = {
            int(row["bin"]): int(row["len"]) for row in counts_df.iter_rows(named=True)
        }
        counts = [counts_by_bin.get(index, 0) for index in range(bin_count)]
        bins = [
            HistogramBin(lower=edges[i], upper=edges[i + 1], count=counts[i])
            for i in range(bin_count)
        ]
        histograms.append(Histogram(bin_count=bin_count, bins=bins))
    return histograms


def build_categorical_histograms(
    values: pl.Series, unique_count: int
) -> list[LabeledHistogram]:
    series = values.drop_nulls()
    if series.len() == 0:
        return []
    if series.name != "value":
        series = series.rename("value")
    counts = series.value_counts().sort(["count", "value"], descending=[True, False])
    if unique_count <= TOP_VALUES_LIMIT:
        labels = [str(row["value"]) for row in counts.iter_rows(named=True)]
        hist_counts = [int(row["count"]) for row in counts.iter_rows(named=True)]
    else:
        top = counts.head(TOP_VALUES_LIMIT)
        labels = [str(row["value"]) for row in top.iter_rows(named=True)]
        hist_counts = [int(row["count"]) for row in top.iter_rows(named=True)]
        suppressed = counts.slice(TOP_VALUES_LIMIT).get_column("count").sum()
        suppressed_count = int(suppressed) if suppressed is not None else 0
        if suppressed_count:
            labels.append("Other")
            hist_counts.append(suppressed_count)
    return [LabeledHistogram(bin_count=len(labels), labels=labels, counts=hist_counts)]


def normalize_datetime_values(
    values: pl.Series,
) -> tuple[pl.Series, pl.Series, bool]:
    series = values.drop_nulls()
    if series.len() == 0:
        return (
            pl.Series("value", [], dtype=pl.Utf8),
            pl.Series("numeric", [], dtype=pl.Float64),
            False,
        )

    is_time = series.dtype == pl.Time
    if is_time:
        normalized = series.cast(pl.Utf8)
        numeric = series.cast(pl.Int64) / 1_000_000_000
        if normalized.name != "value":
            normalized = normalized.rename("value")
        return normalized, numeric.cast(pl.Float64).rename("numeric"), True

    if series.dtype == pl.Datetime:
        date_series = series.dt.date()
    else:
        date_series = series.cast(pl.Date)
    normalized = date_series.cast(pl.Utf8)
    numeric = date_series.cast(pl.Int64).cast(pl.Float64)
    if normalized.name != "value":
        normalized = normalized.rename("value")
    return normalized, numeric.rename("numeric"), False


def build_datetime_histograms(
    values: pl.Series, *, is_time: bool
) -> list[LabeledHistogram]:
    if values.len() == 0:
        return []
    base_histograms = build_numeric_histograms(values, is_integer=not is_time)
    labeled_histograms: list[LabeledHistogram] = []
    for histogram in base_histograms:
        labels = []
        counts = []
        for bin_item in histogram.bins:
            label = format_datetime_bin(bin_item.lower, bin_item.upper, is_time)
            labels.append(label)
            counts.append(bin_item.count)
        labeled_histograms.append(
            LabeledHistogram(
                bin_count=histogram.bin_count,
                labels=labels,
                counts=counts,
            )
        )
    return labeled_histograms


def format_datetime_bin(lower: float, upper: float, is_time: bool) -> str:
    if is_time:
        lower_label = _format_time_label(lower)
        upper_label = _format_time_label(upper)
    else:
        lower_label = _format_date_label(lower)
        upper_label = _format_date_label(upper)
    return (
        lower_label if lower_label == upper_label else f"{lower_label} - {upper_label}"
    )


def _format_date_label(value: float) -> str:
    epoch = dt.date(1970, 1, 1)
    ordinal = int(round(value))
    return (epoch + dt.timedelta(days=ordinal)).isoformat()


def _require_float_value(value: object | None) -> float:
    if value is None:
        raise ValueError("value is required")
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    raise TypeError("value must be numeric")


def _format_time_label(value: float) -> str:
    time_value = (dt.datetime(1970, 1, 1) + dt.timedelta(seconds=value)).time()
    timespec = "microseconds" if time_value.microsecond else "seconds"
    return time_value.isoformat(timespec=timespec)
