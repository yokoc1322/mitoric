"""Categorical column profiling."""

from __future__ import annotations

import polars as pl

from mitoric.models.aggregation import CategoricalProfile, CategoryCount
from mitoric.models.base import SuppressedCount
from mitoric.profiling.histograms.builder import build_categorical_histograms
from mitoric.profiling.utils.constants import TOP_VALUES_LIMIT
from mitoric.profiling.utils.type_utils import TEXT_CARDINALITY_THRESHOLD


def build_categorical_profile(
    values: pl.Series, unique_count: int
) -> CategoricalProfile:
    series = values.drop_nulls()
    if series.name != "value":
        series = series.rename("value")
    counts = series.value_counts().sort(["count", "value"], descending=[True, False])
    top_categories = [
        CategoryCount(category=str(row["value"]), count=int(row["count"]))
        for row in counts.head(TOP_VALUES_LIMIT).iter_rows(named=True)
    ]
    is_high = unique_count > TEXT_CARDINALITY_THRESHOLD
    suppressed = unique_count - len(top_categories) if is_high else 0
    return CategoricalProfile(
        top_categories=top_categories,
        is_high_cardinality=is_high,
        suppressed_count=SuppressedCount(suppressed),
        histograms=build_categorical_histograms(values, unique_count),
    )
