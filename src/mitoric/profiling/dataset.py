"""Dataset-level profiling."""

from __future__ import annotations

from collections import Counter

import polars as pl

from mitoric.models.aggregation import (
    ColumnMatchSummary,
    ComparisonSummary,
    DatasetSummary,
    TypeCounts,
    TypeMismatch,
)
from mitoric.models.base import (
    ColumnCount,
    ColumnName,
    ColumnType,
    DatasetId,
    DuplicateRowCount,
    MemoryBytes,
    MissingCount,
    MissingRate,
    RowCount,
    RowCountDelta,
)
from mitoric.profiling.utils.type_utils import classify_column_type, infer_column_type


def summarize_dataset(frame: pl.DataFrame, dataset_id: str) -> DatasetSummary:
    row_count = frame.height
    column_count = frame.width
    total_cells = row_count * column_count

    null_counts = frame.null_count().row(0) if column_count > 0 else []
    missing_cells = sum(int(value) for value in null_counts)
    missing_rate = missing_cells / total_cells if total_cells else 0.0

    duplicate_rows = row_count - frame.unique().height if row_count else 0

    type_counter: Counter[ColumnType] = Counter()
    for name in frame.columns:
        series = frame.get_column(name)
        type_counter[classify_column_type(series)] += 1

    type_counts = TypeCounts(
        numeric=ColumnCount(type_counter.get(ColumnType.NUMERIC, 0)),
        categorical=ColumnCount(type_counter.get(ColumnType.CATEGORICAL, 0)),
        text=ColumnCount(type_counter.get(ColumnType.TEXT, 0)),
        datetime=ColumnCount(type_counter.get(ColumnType.DATETIME, 0)),
        boolean=ColumnCount(type_counter.get(ColumnType.BOOLEAN, 0)),
    )

    return DatasetSummary(
        dataset_id=DatasetId(dataset_id),
        row_count=RowCount(row_count),
        column_count=ColumnCount(column_count),
        memory_bytes=MemoryBytes(int(frame.estimated_size())),
        missing_cells=MissingCount(missing_cells),
        missing_rate=MissingRate(missing_rate),
        duplicate_rows=DuplicateRowCount(duplicate_rows),
        type_counts=type_counts,
    )


def summarize_comparison(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    left_id: str,
    right_id: str,
) -> ComparisonSummary:
    left_summary = summarize_dataset(left, dataset_id=left_id)
    right_summary = summarize_dataset(right, dataset_id=right_id)

    left_columns = set(left.columns)
    right_columns = set(right.columns)
    matched = sorted(left_columns & right_columns)
    left_only = sorted(left_columns - right_columns)
    right_only = sorted(right_columns - left_columns)

    mismatches: list[TypeMismatch] = []
    for name in matched:
        left_type = infer_column_type(left.get_column(name))
        right_type = infer_column_type(right.get_column(name))
        if left_type != right_type:
            mismatches.append(
                TypeMismatch(
                    column_name=ColumnName(name),
                    left_type=left_type,
                    right_type=right_type,
                )
            )

    column_matches = ColumnMatchSummary(
        matched=ColumnCount(len(matched)),
        left_only=ColumnCount(len(left_only)),
        right_only=ColumnCount(len(right_only)),
        total_left=ColumnCount(len(left_columns)),
        total_right=ColumnCount(len(right_columns)),
        column_names_left_only=[ColumnName(name) for name in left_only],
        column_names_right_only=[ColumnName(name) for name in right_only],
    )

    return ComparisonSummary(
        left_dataset=left_summary,
        right_dataset=right_summary,
        row_count_delta=RowCountDelta(left.height - right.height),
        column_matches=column_matches,
        type_mismatches=mismatches,
        column_profiles_left_only=[],
        column_profiles_right_only=[],
    )
