"""Association profiling."""

from __future__ import annotations

import math

import polars as pl

from mitoric.models.aggregation import Association, AssociationSummary
from mitoric.models.base import AssociationValue, ColumnName, ColumnType
from mitoric.profiling.utils.type_utils import classify_column_type

_TOP_ASSOCIATIONS = 20
_MAX_ASSOCIATION_ROWS = 50_000


def _limit_association_rows(frame: pl.DataFrame) -> pl.DataFrame:
    if frame.height > _MAX_ASSOCIATION_ROWS:
        return frame.head(_MAX_ASSOCIATION_ROWS)
    return frame


def _pearson(frame: pl.DataFrame, left: str, right: str) -> float:
    subset = frame.select([pl.col(left), pl.col(right)]).drop_nulls()
    if subset.height < 2:
        return 0.0
    cov, var_left, var_right = subset.select(
        ((pl.col(left) - pl.col(left).mean()) * (pl.col(right) - pl.col(right).mean()))
        .sum()
        .alias("cov"),
        ((pl.col(left) - pl.col(left).mean()) ** 2).sum().alias("var_left"),
        ((pl.col(right) - pl.col(right).mean()) ** 2).sum().alias("var_right"),
    ).row(0)
    if cov is None or var_left is None or var_right is None:
        return 0.0
    if var_left == 0 or var_right == 0:
        return 0.0
    return float(cov) / math.sqrt(float(var_left) * float(var_right))


def _cramers_v(frame: pl.DataFrame, left: str, right: str) -> float:
    subset = frame.select([pl.col(left), pl.col(right)]).drop_nulls()
    if subset.height == 0:
        return 0.0
    counts = subset.group_by([left, right]).len().rename({"len": "count"})
    if counts.height == 0:
        return 0.0
    row_values = subset.select(pl.col(left).unique())
    col_values = subset.select(pl.col(right).unique())
    full_table = (
        row_values.join(col_values, how="cross")
        .join(counts, on=[left, right], how="left")
        .with_columns(pl.col("count").fill_null(0))
    )
    row_totals = full_table.group_by(left).agg(pl.sum("count").alias("row_total"))
    col_totals = full_table.group_by(right).agg(pl.sum("count").alias("col_total"))
    k = min(row_totals.height, col_totals.height)
    if k <= 1:
        return 0.0
    n = full_table.select(pl.sum("count")).row(0)[0]
    if not n:
        return 0.0
    joined = full_table.join(row_totals, on=left).join(col_totals, on=right)
    expected = pl.col("row_total") * pl.col("col_total") / pl.lit(n)
    chi2 = joined.select(((pl.col("count") - expected) ** 2 / expected).sum()).row(0)[0]
    if chi2 is None or chi2 == 0:
        return 0.0
    return math.sqrt(float(chi2) / (float(n) * (k - 1)))


def _correlation_ratio(frame: pl.DataFrame, numeric: str, categorical: str) -> float:
    subset = frame.select([pl.col(numeric), pl.col(categorical)]).drop_nulls()
    if subset.height == 0:
        return 0.0
    mean_total = subset.select(pl.col(numeric).mean()).row(0)[0]
    if mean_total is None:
        return 0.0
    grouped = subset.group_by(categorical).agg(
        pl.col(numeric).mean().alias("group_mean"),
        pl.len().alias("group_count"),
    )
    numerator = grouped.select(
        (pl.col("group_count") * (pl.col("group_mean") - pl.lit(mean_total)) ** 2).sum()
    ).row(0)[0]
    denominator = subset.select(
        ((pl.col(numeric) - pl.lit(mean_total)) ** 2).sum()
    ).row(0)[0]
    if numerator is None or denominator is None or denominator == 0:
        return 0.0
    return math.sqrt(float(numerator) / float(denominator))


def compute_associations(frame: pl.DataFrame) -> AssociationSummary:
    frame = _limit_association_rows(frame)
    numeric_columns: list[str] = []
    categorical_columns: list[str] = []

    for name in frame.columns:
        series = frame.get_column(name)
        kind = classify_column_type(series)
        if kind == ColumnType.BOOLEAN:
            kind = ColumnType.CATEGORICAL
        if kind == ColumnType.NUMERIC:
            numeric_columns.append(name)
        elif kind == ColumnType.CATEGORICAL:
            categorical_columns.append(name)

    numeric_numeric: list[Association] = []
    categorical_categorical: list[Association] = []
    numeric_categorical: list[Association] = []

    for i, left in enumerate(numeric_columns):
        for right in numeric_columns[i + 1 :]:
            value = _pearson(frame, left, right)
            numeric_numeric.append(
                Association(
                    left=ColumnName(left),
                    right=ColumnName(right),
                    value=AssociationValue(value),
                )
            )

    for i, left in enumerate(categorical_columns):
        for right in categorical_columns[i + 1 :]:
            value = _cramers_v(frame, left, right)
            categorical_categorical.append(
                Association(
                    left=ColumnName(left),
                    right=ColumnName(right),
                    value=AssociationValue(value),
                )
            )

    for numeric in numeric_columns:
        for categorical in categorical_columns:
            value = _correlation_ratio(frame, numeric, categorical)
            numeric_categorical.append(
                Association(
                    left=ColumnName(numeric),
                    right=ColumnName(categorical),
                    value=AssociationValue(value),
                )
            )

    def _top(entries: list[Association]) -> list[Association]:
        ordered = sorted(entries, key=lambda item: item.value, reverse=True)
        return ordered[:_TOP_ASSOCIATIONS]

    return AssociationSummary(
        numeric_numeric=_top(numeric_numeric),
        categorical_categorical=_top(categorical_categorical),
        numeric_categorical=_top(numeric_categorical),
    )
