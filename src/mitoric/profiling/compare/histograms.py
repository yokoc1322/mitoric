"""Histogram comparison utilities."""

from __future__ import annotations

import math
from decimal import Decimal

import polars as pl

from mitoric.models.aggregation import CompareHistogram
from mitoric.models.base import ColumnType
from mitoric.profiling.histograms.builder import (
    format_datetime_bin,
    normalize_datetime_values,
)
from mitoric.profiling.histograms.config import HISTOGRAM_BINS
from mitoric.profiling.utils.constants import TOP_VALUES_LIMIT
from mitoric.profiling.utils.type_utils import (
    normalize_numeric_series,
)
from mitoric.render.formatters import _format_number_label, _format_numeric_bin_label


def build_compare_numeric_histograms(
    left_values: pl.Series,
    right_values: pl.Series,
    *,
    is_integer: bool,
) -> list[CompareHistogram]:
    left_series = left_values.drop_nulls()
    right_series = right_values.drop_nulls()
    if left_series.len() == 0 and right_series.len() == 0:
        return [
            CompareHistogram(
                bin_count=bin_count,
                labels=[],
                left_counts=[],
                right_counts=[],
            )
            for bin_count in HISTOGRAM_BINS
        ]

    left_series = (
        left_series.rename("value") if left_series.name != "value" else left_series
    )
    right_series = (
        right_series.rename("value") if right_series.name != "value" else right_series
    )
    all_series = pl.concat([left_series, right_series], how="vertical")
    unique_series = all_series.unique().sort()
    if unique_series.len() <= TOP_VALUES_LIMIT:
        left_counts_map = _value_counts_map(left_series)
        right_counts_map = _value_counts_map(right_series)
        unique_values = unique_series.to_list()
        labels = [
            _format_number_label(_required_float(value), is_integer)
            for value in unique_values
        ]
        left_counts = [left_counts_map.get(value, 0) for value in unique_values]
        right_counts = [right_counts_map.get(value, 0) for value in unique_values]
        return [
            CompareHistogram(
                bin_count=len(unique_values),
                labels=labels,
                left_counts=left_counts,
                right_counts=right_counts,
            )
        ]

    min_value = _required_float(all_series.min())
    max_value = _required_float(all_series.max())
    span = max_value - min_value
    histograms: list[CompareHistogram] = []
    for bin_count in HISTOGRAM_BINS:
        if span == 0:
            label = _format_number_label(min_value, is_integer)
            histograms.append(
                CompareHistogram(
                    bin_count=bin_count,
                    labels=[label],
                    left_counts=[left_series.len()],
                    right_counts=[right_series.len()],
                )
            )
            continue
        if is_integer:
            integer_min = int(min_value)
            integer_max = int(max_value)
            range_size = integer_max - integer_min + 1
            width = max(1, math.ceil(range_size / bin_count))
            edges = [integer_min + width * i for i in range(bin_count + 1)]
            bin_expr = (
                ((pl.col("value").cast(pl.Int64) - integer_min) // width)
                .clip(lower_bound=0, upper_bound=bin_count - 1)
                .alias("bin")
            )
            left_counts_map = _bin_counts_map(left_series, bin_expr)
            right_counts_map = _bin_counts_map(right_series, bin_expr)
            left_counts = [left_counts_map.get(i, 0) for i in range(bin_count)]
            right_counts = [right_counts_map.get(i, 0) for i in range(bin_count)]
            labels = []
            for i in range(bin_count):
                lower = float(edges[i])
                upper = float(max(edges[i], min(edges[i + 1] - 1, integer_max)))
                labels.append(_format_numeric_bin_label(lower, upper, True))
            histograms.append(
                CompareHistogram(
                    bin_count=bin_count,
                    labels=labels,
                    left_counts=left_counts,
                    right_counts=right_counts,
                )
            )
            continue

        width = span / bin_count
        edges = [min_value + width * i for i in range(bin_count + 1)]
        bin_expr = (
            pl.when(pl.col("value") == max_value)
            .then(bin_count - 1)
            .otherwise(((pl.col("value") - min_value) / width).cast(pl.Int64))
            .alias("bin")
        )
        left_counts_map = _bin_counts_map(left_series, bin_expr)
        right_counts_map = _bin_counts_map(right_series, bin_expr)
        left_counts = [left_counts_map.get(i, 0) for i in range(bin_count)]
        right_counts = [right_counts_map.get(i, 0) for i in range(bin_count)]
        labels = [
            _format_numeric_bin_label(edges[i], edges[i + 1], False)
            for i in range(bin_count)
        ]
        histograms.append(
            CompareHistogram(
                bin_count=bin_count,
                labels=labels,
                left_counts=left_counts,
                right_counts=right_counts,
            )
        )
    return histograms


def build_compare_categorical_histograms(
    left_values: pl.Series, right_values: pl.Series
) -> list[CompareHistogram]:
    left_series = left_values.drop_nulls()
    right_series = right_values.drop_nulls()
    if left_series.len() == 0 and right_series.len() == 0:
        return []
    left_series = (
        left_series.rename("value") if left_series.name != "value" else left_series
    )
    right_series = (
        right_series.rename("value") if right_series.name != "value" else right_series
    )
    combined = pl.concat([left_series, right_series], how="vertical")
    counts = combined.value_counts().sort(["count", "value"], descending=[True, False])
    left_counts_map = _value_counts_map(left_series)
    right_counts_map = _value_counts_map(right_series)
    if counts.height <= TOP_VALUES_LIMIT:
        labels = [str(row["value"]) for row in counts.iter_rows(named=True)]
        left_counts = [left_counts_map.get(label, 0) for label in labels]
        right_counts = [right_counts_map.get(label, 0) for label in labels]
        return [
            CompareHistogram(
                bin_count=len(labels),
                labels=labels,
                left_counts=left_counts,
                right_counts=right_counts,
            )
        ]
    top = counts.head(TOP_VALUES_LIMIT)
    labels = [str(row["value"]) for row in top.iter_rows(named=True)]
    left_counts = [left_counts_map.get(label, 0) for label in labels]
    right_counts = [right_counts_map.get(label, 0) for label in labels]
    left_other = left_series.len() - sum(left_counts)
    right_other = right_series.len() - sum(right_counts)
    if left_other or right_other:
        labels.append("Other")
        left_counts.append(left_other)
        right_counts.append(right_other)
    return [
        CompareHistogram(
            bin_count=len(labels),
            labels=labels,
            left_counts=left_counts,
            right_counts=right_counts,
        )
    ]


def build_compare_text_length_histograms(
    left_values: pl.Series, right_values: pl.Series
) -> list[CompareHistogram]:
    left_lengths = left_values.drop_nulls().str.len_chars().cast(pl.Float64)
    right_lengths = right_values.drop_nulls().str.len_chars().cast(pl.Float64)
    return build_compare_numeric_histograms(
        left_lengths, right_lengths, is_integer=True
    )


def build_compare_datetime_histograms(
    left_values: pl.Series,
    right_values: pl.Series,
) -> list[CompareHistogram]:
    left_series = left_values.drop_nulls()
    right_series = right_values.drop_nulls()
    if left_series.len() == 0 and right_series.len() == 0:
        return []
    _, left_numeric, left_is_time = normalize_datetime_values(left_series)
    _, right_numeric, right_is_time = normalize_datetime_values(right_series)
    if left_is_time != right_is_time:
        return []

    if left_numeric.len() == 0 and right_numeric.len() == 0:
        return []
    left_numeric = (
        left_numeric.rename("value") if left_numeric.name != "value" else left_numeric
    )
    right_numeric = (
        right_numeric.rename("value")
        if right_numeric.name != "value"
        else right_numeric
    )
    all_values = pl.concat([left_numeric, right_numeric], how="vertical")
    unique_series = all_values.unique().sort()
    if unique_series.len() <= TOP_VALUES_LIMIT:
        left_counts_map = _value_counts_map(left_numeric)
        right_counts_map = _value_counts_map(right_numeric)
        unique_values = unique_series.to_list()
        labels = [
            format_datetime_bin(
                _required_float(value), _required_float(value), left_is_time
            )
            for value in unique_values
        ]
        left_counts = [left_counts_map.get(value, 0) for value in unique_values]
        right_counts = [right_counts_map.get(value, 0) for value in unique_values]
        return [
            CompareHistogram(
                bin_count=len(unique_values),
                labels=labels,
                left_counts=left_counts,
                right_counts=right_counts,
            )
        ]

    min_value = _required_float(all_values.min())
    max_value = _required_float(all_values.max())
    span = max_value - min_value
    histograms: list[CompareHistogram] = []
    for bin_count in HISTOGRAM_BINS:
        if span == 0:
            label = format_datetime_bin(min_value, min_value, left_is_time)
            histograms.append(
                CompareHistogram(
                    bin_count=bin_count,
                    labels=[label],
                    left_counts=[left_numeric.len()],
                    right_counts=[right_numeric.len()],
                )
            )
            continue
        if left_is_time:
            time_width = span / bin_count
            edges = [min_value + time_width * i for i in range(bin_count + 1)]
            bin_expr = (
                pl.when(pl.col("value") == max_value)
                .then(bin_count - 1)
                .otherwise(((pl.col("value") - min_value) / time_width).cast(pl.Int64))
                .alias("bin")
            )
        else:
            integer_min = int(min_value)
            integer_max = int(max_value)
            range_size = integer_max - integer_min + 1
            date_width = max(1, math.ceil(range_size / bin_count))
            edges = [integer_min + date_width * i for i in range(bin_count + 1)]
            bin_expr = (
                ((pl.col("value").cast(pl.Int64) - integer_min) // date_width)
                .clip(lower_bound=0, upper_bound=bin_count - 1)
                .alias("bin")
            )

        left_counts_map = _bin_counts_map(left_numeric, bin_expr)
        right_counts_map = _bin_counts_map(right_numeric, bin_expr)
        left_counts = [left_counts_map.get(i, 0) for i in range(bin_count)]
        right_counts = [right_counts_map.get(i, 0) for i in range(bin_count)]
        labels = [
            format_datetime_bin(edges[i], edges[i + 1], left_is_time)
            for i in range(bin_count)
        ]
        histograms.append(
            CompareHistogram(
                bin_count=bin_count,
                labels=labels,
                left_counts=left_counts,
                right_counts=right_counts,
            )
        )
    return histograms


def build_compare_histograms_for_column(
    left_series: pl.Series,
    right_series: pl.Series,
    left_type: ColumnType,
    right_type: ColumnType,
) -> list[CompareHistogram]:
    if left_type != right_type:
        return []
    if left_type == ColumnType.NUMERIC:
        left_numeric, left_is_integer = normalize_numeric_series(left_series)
        right_numeric, right_is_integer = normalize_numeric_series(right_series)
        is_integer = left_is_integer and right_is_integer
        return build_compare_numeric_histograms(
            left_numeric.drop_nulls().cast(pl.Float64),
            right_numeric.drop_nulls().cast(pl.Float64),
            is_integer=is_integer,
        )
    if left_type in (ColumnType.CATEGORICAL, ColumnType.BOOLEAN):
        left_values = left_series.drop_nulls().cast(pl.Utf8)
        right_values = right_series.drop_nulls().cast(pl.Utf8)
        if left_type == ColumnType.BOOLEAN:
            left_values = left_values.str.to_titlecase()
            right_values = right_values.str.to_titlecase()
        return build_compare_categorical_histograms(left_values, right_values)
    if left_type == ColumnType.TEXT:
        return build_compare_text_length_histograms(
            left_series.drop_nulls().cast(pl.Utf8),
            right_series.drop_nulls().cast(pl.Utf8),
        )
    if left_type == ColumnType.DATETIME:
        return build_compare_datetime_histograms(
            left_series.drop_nulls(),
            right_series.drop_nulls(),
        )
    return []


def _value_counts_map(series: pl.Series) -> dict[float | str, int]:
    if series.len() == 0:
        return {}
    counts = series.value_counts()
    return {row["value"]: int(row["count"]) for row in counts.iter_rows(named=True)}


def _bin_counts_map(series: pl.Series, bin_expr: pl.Expr) -> dict[int, int]:
    if series.len() == 0:
        return {}
    df = (
        pl.DataFrame({"value": series})
        .with_columns(bin_expr)
        .group_by("bin")
        .len()
        .sort("bin")
    )
    return {int(row["bin"]): int(row["len"]) for row in df.iter_rows(named=True)}


def _required_float(value: object | None) -> float:
    if value is None:
        raise ValueError("value is required")
    if isinstance(value, bool):
        return float(int(value))
    if isinstance(value, (int, float, Decimal)):
        return float(value)
    raise TypeError("value must be numeric")
