"""Typed payload schema for report rendering."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Literal, TypedDict

from mitoric.models.base import (
    AssociationValue,
    ColumnCount,
    ColumnName,
    ColumnType,
    DatasetId,
    DuplicateRowCount,
    MemoryBytes,
    MissingCount,
    MissingRate,
    NonNullCount,
    NullCount,
    NullRate,
    OutlierRate,
    RowCount,
    RowCountDelta,
    SuppressedCount,
    UniqueCount,
    WarningMessage,
    ZeroCount,
)


class TypeCountsPayload(TypedDict):
    numeric: ColumnCount
    categorical: ColumnCount
    text: ColumnCount
    datetime: ColumnCount
    boolean: ColumnCount


class DatasetSummaryPayload(TypedDict):
    dataset_id: DatasetId
    row_count: RowCount
    column_count: ColumnCount
    memory_bytes: MemoryBytes
    missing_cells: MissingCount
    missing_rate: MissingRate
    duplicate_rows: DuplicateRowCount
    type_counts: TypeCountsPayload


class QuantileValuePayload(TypedDict):
    quantile: float
    value: float


class NumericStatsPayload(TypedDict):
    minimum: float
    maximum: float
    mean: float
    median: float
    std: float
    variance: float
    quantiles: list[QuantileValuePayload]
    iqr: float


class HistogramBinPayload(TypedDict):
    lower: float
    upper: float
    count: int


class HistogramPayload(TypedDict):
    bin_count: int
    bins: list[HistogramBinPayload]


class LabeledHistogramPayload(TypedDict):
    bin_count: int
    labels: list[str]
    counts: list[int]


class CompareHistogramPayload(TypedDict):
    bin_count: int
    labels: list[str]
    left_counts: list[int]
    right_counts: list[int]


class NumericValueCountPayload(TypedDict):
    value: float
    count: int


class NumericProfilePayload(TypedDict):
    is_integer: bool
    stats: NumericStatsPayload
    outlier_rate: OutlierRate
    histograms: list[HistogramPayload]
    top_values: list[NumericValueCountPayload]
    min_values: list[NumericValueCountPayload]
    max_values: list[NumericValueCountPayload]


class CategoryCountPayload(TypedDict):
    category: str
    count: int


class CategoricalProfilePayload(TypedDict):
    top_categories: list[CategoryCountPayload]
    is_high_cardinality: bool
    suppressed_count: SuppressedCount
    histograms: list[LabeledHistogramPayload]


class TextLengthStatsPayload(TypedDict):
    mean: float
    median: float
    minimum: int
    maximum: int


class TokenCountPayload(TypedDict):
    token: str
    count: int


class TextProfilePayload(TypedDict):
    length_stats: TextLengthStatsPayload
    top_tokens: list[TokenCountPayload]
    length_histograms: list[HistogramPayload]


class DatetimeValueCountPayload(TypedDict):
    value: str
    count: int


class DatetimeProfilePayload(TypedDict):
    min_datetime: str
    max_datetime: str
    histograms: list[LabeledHistogramPayload]
    top_values: list[DatetimeValueCountPayload]


class ColumnProfilePayload(TypedDict):
    column_name: ColumnName
    data_type: ColumnType
    non_null_count: NonNullCount
    null_count: NullCount
    null_rate: NullRate
    unique_count: UniqueCount
    zero_count: ZeroCount
    numeric_profile: NumericProfilePayload | None
    categorical_profile: CategoricalProfilePayload | None
    text_profile: TextProfilePayload | None
    datetime_profile: DatetimeProfilePayload | None


class CompareColumnProfilePayload(TypedDict):
    column_name: ColumnName
    left_profile: ColumnProfilePayload
    right_profile: ColumnProfilePayload
    histograms: list[CompareHistogramPayload]


class AssociationPayload(TypedDict):
    left: ColumnName
    right: ColumnName
    value: AssociationValue


class AssociationSummaryPayload(TypedDict):
    numeric_numeric: list[AssociationPayload]
    categorical_categorical: list[AssociationPayload]
    numeric_categorical: list[AssociationPayload]


class ColumnMatchSummaryPayload(TypedDict):
    matched: ColumnCount
    left_only: ColumnCount
    right_only: ColumnCount
    total_left: ColumnCount
    total_right: ColumnCount
    column_names_left_only: list[ColumnName]
    column_names_right_only: list[ColumnName]


class TypeMismatchPayload(TypedDict):
    column_name: ColumnName
    left_type: ColumnType
    right_type: ColumnType


class ComparisonSummaryPayload(TypedDict):
    left_dataset: DatasetSummaryPayload
    right_dataset: DatasetSummaryPayload
    row_count_delta: RowCountDelta
    column_matches: ColumnMatchSummaryPayload
    type_mismatches: list[TypeMismatchPayload]
    column_profiles_left_only: list[ColumnProfilePayload]
    column_profiles_right_only: list[ColumnProfilePayload]


class SingleReportPayload(TypedDict):
    mode: Literal["single"]
    warnings: list[WarningMessage]
    dataset_summary: DatasetSummaryPayload
    column_profiles: list[ColumnProfilePayload]
    associations: AssociationSummaryPayload
    histogram_bins: Sequence[int]


class CompareReportPayload(TypedDict):
    mode: Literal["compare"]
    warnings: list[WarningMessage]
    comparison_summary: ComparisonSummaryPayload
    compare_column_profiles: list[CompareColumnProfilePayload]
    histogram_bins: Sequence[int]


ReportPayload = SingleReportPayload | CompareReportPayload
