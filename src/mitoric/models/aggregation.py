"""Aggregate profiling structures used to build report payloads."""

from __future__ import annotations

from dataclasses import dataclass, field

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
    ZeroCount,
)


@dataclass(frozen=True)
class TypeCounts:
    numeric: ColumnCount
    categorical: ColumnCount
    text: ColumnCount
    datetime: ColumnCount
    boolean: ColumnCount


@dataclass(frozen=True)
class DatasetSummary:
    dataset_id: DatasetId
    row_count: RowCount
    column_count: ColumnCount
    memory_bytes: MemoryBytes
    missing_cells: MissingCount
    missing_rate: MissingRate
    duplicate_rows: DuplicateRowCount
    type_counts: TypeCounts


@dataclass(frozen=True)
class QuantileValue:
    quantile: float
    value: float


@dataclass(frozen=True)
class NumericStats:
    minimum: float
    maximum: float
    mean: float
    median: float
    std: float
    variance: float
    quantiles: list[QuantileValue]
    iqr: float


@dataclass(frozen=True)
class HistogramBin:
    lower: float
    upper: float
    count: int


@dataclass(frozen=True)
class Histogram:
    bin_count: int
    bins: list[HistogramBin]


@dataclass(frozen=True)
class LabeledHistogram:
    bin_count: int
    labels: list[str]
    counts: list[int]


@dataclass(frozen=True)
class CompareHistogram:
    bin_count: int
    labels: list[str]
    left_counts: list[int]
    right_counts: list[int]


@dataclass(frozen=True)
class NumericValueCount:
    value: float
    count: int


@dataclass(frozen=True)
class NumericProfile:
    is_integer: bool
    stats: NumericStats
    outlier_rate: OutlierRate
    histograms: list[Histogram]
    top_values: list[NumericValueCount]
    min_values: list[NumericValueCount]
    max_values: list[NumericValueCount]


@dataclass(frozen=True)
class CategoryCount:
    category: str
    count: int


@dataclass(frozen=True)
class CategoricalProfile:
    top_categories: list[CategoryCount]
    is_high_cardinality: bool
    suppressed_count: SuppressedCount
    histograms: list[LabeledHistogram] = field(default_factory=list)


@dataclass(frozen=True)
class TextLengthStats:
    mean: float
    median: float
    minimum: int
    maximum: int


@dataclass(frozen=True)
class TokenCount:
    token: str
    count: int


@dataclass(frozen=True)
class TextProfile:
    length_stats: TextLengthStats
    top_tokens: list[TokenCount]
    length_histograms: list[Histogram] = field(default_factory=list)


@dataclass(frozen=True)
class ListLengthStats:
    mean: float
    median: float
    minimum: int
    maximum: int


@dataclass(frozen=True)
class ListProfile:
    length_stats: ListLengthStats
    length_histograms: list[Histogram] = field(default_factory=list)
    value_samples: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class DatetimeValueCount:
    value: str
    count: int


@dataclass(frozen=True)
class DatetimeProfile:
    min_datetime: str
    max_datetime: str
    histograms: list[LabeledHistogram] = field(default_factory=list)
    top_values: list[DatetimeValueCount] = field(default_factory=list)


@dataclass(frozen=True)
class ColumnProfile:
    column_name: ColumnName
    data_type: ColumnType
    non_null_count: NonNullCount
    null_count: NullCount
    null_rate: NullRate
    unique_count: UniqueCount
    zero_count: ZeroCount
    numeric_profile: NumericProfile | None = None
    categorical_profile: CategoricalProfile | None = None
    text_profile: TextProfile | None = None
    datetime_profile: DatetimeProfile | None = None
    list_profile: ListProfile | None = None
    value_samples: list[str] = field(default_factory=list)


@dataclass(frozen=True)
class CompareColumnProfile:
    column_name: ColumnName
    left_profile: ColumnProfile
    right_profile: ColumnProfile
    histograms: list[CompareHistogram] = field(default_factory=list)


@dataclass(frozen=True)
class Association:
    left: ColumnName
    right: ColumnName
    value: AssociationValue


@dataclass(frozen=True)
class AssociationSummary:
    numeric_numeric: list[Association]
    categorical_categorical: list[Association]
    numeric_categorical: list[Association]


@dataclass(frozen=True)
class ColumnMatchSummary:
    matched: ColumnCount
    left_only: ColumnCount
    right_only: ColumnCount
    total_left: ColumnCount
    total_right: ColumnCount
    column_names_left_only: list[ColumnName] = field(default_factory=list)
    column_names_right_only: list[ColumnName] = field(default_factory=list)


@dataclass(frozen=True)
class TypeMismatch:
    column_name: ColumnName
    left_type: ColumnType
    right_type: ColumnType


@dataclass(frozen=True)
class ComparisonSummary:
    left_dataset: DatasetSummary
    right_dataset: DatasetSummary
    row_count_delta: RowCountDelta
    column_matches: ColumnMatchSummary
    type_mismatches: list[TypeMismatch]
    column_profiles_left_only: list[ColumnProfile]
    column_profiles_right_only: list[ColumnProfile]
