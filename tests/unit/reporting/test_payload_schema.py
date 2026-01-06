from __future__ import annotations

from mitoric.models.aggregation import (
    Association,
    AssociationSummary,
    CategoricalProfile,
    CategoryCount,
    ColumnMatchSummary,
    ColumnProfile,
    CompareColumnProfile,
    CompareHistogram,
    ComparisonSummary,
    DatasetSummary,
    DatetimeProfile,
    DatetimeValueCount,
    Histogram,
    HistogramBin,
    LabeledHistogram,
    ListLengthStats,
    ListProfile,
    NumericProfile,
    NumericStats,
    NumericValueCount,
    QuantileValue,
    TextLengthStats,
    TextProfile,
    TokenCount,
    TypeCounts,
    TypeMismatch,
)
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
from mitoric.reporting.builder import (
    build_compare_report_payload,
    build_single_report_payload,
)

_EXPECTED_SINGLE_SCHEMA = {
    "mode": "value",
    "warnings": ["value"],
    "dataset_summary": {
        "dataset_id": "value",
        "row_count": "value",
        "column_count": "value",
        "memory_bytes": "value",
        "missing_cells": "value",
        "missing_rate": "value",
        "duplicate_rows": "value",
        "type_counts": {
            "numeric": "value",
            "categorical": "value",
            "text": "value",
            "datetime": "value",
            "boolean": "value",
        },
    },
    "column_profiles": [
        {
            "column_name": "value",
            "data_type": "value",
            "non_null_count": "value",
            "null_count": "value",
            "null_rate": "value",
            "unique_count": "value",
            "zero_count": "value",
            "value_samples": ["value"],
            "numeric_profile": {
                "is_integer": "value",
                "stats": {
                    "minimum": "value",
                    "maximum": "value",
                    "mean": "value",
                    "median": "value",
                    "std": "value",
                    "variance": "value",
                    "quantiles": [
                        {
                            "quantile": "value",
                            "value": "value",
                        }
                    ],
                    "iqr": "value",
                },
                "outlier_rate": "value",
                "histograms": [
                    {
                        "bin_count": "value",
                        "bins": [
                            {
                                "lower": "value",
                                "upper": "value",
                                "count": "value",
                            }
                        ],
                    }
                ],
                "top_values": [{"value": "value", "count": "value"}],
                "min_values": [{"value": "value", "count": "value"}],
                "max_values": [{"value": "value", "count": "value"}],
            },
            "categorical_profile": {
                "top_categories": [{"category": "value", "count": "value"}],
                "is_high_cardinality": "value",
                "suppressed_count": "value",
                "histograms": [
                    {
                        "bin_count": "value",
                        "labels": ["value"],
                        "counts": ["value"],
                    }
                ],
            },
            "text_profile": {
                "length_stats": {
                    "mean": "value",
                    "median": "value",
                    "minimum": "value",
                    "maximum": "value",
                },
                "top_tokens": [{"token": "value", "count": "value"}],
                "length_histograms": [
                    {
                        "bin_count": "value",
                        "bins": [
                            {
                                "lower": "value",
                                "upper": "value",
                                "count": "value",
                            }
                        ],
                    }
                ],
            },
            "datetime_profile": {
                "min_datetime": "value",
                "max_datetime": "value",
                "histograms": [
                    {
                        "bin_count": "value",
                        "labels": ["value"],
                        "counts": ["value"],
                    }
                ],
                "top_values": [{"value": "value", "count": "value"}],
            },
            "list_profile": {
                "length_stats": {
                    "mean": "value",
                    "median": "value",
                    "minimum": "value",
                    "maximum": "value",
                },
                "length_histograms": [
                    {
                        "bin_count": "value",
                        "bins": [
                            {
                                "lower": "value",
                                "upper": "value",
                                "count": "value",
                            }
                        ],
                    }
                ],
                "value_samples": ["value"],
            },
        }
    ],
    "associations": {
        "numeric_numeric": [{"left": "value", "right": "value", "value": "value"}],
        "categorical_categorical": [
            {"left": "value", "right": "value", "value": "value"}
        ],
        "numeric_categorical": [{"left": "value", "right": "value", "value": "value"}],
    },
    "histogram_bins": ["value"],
}

_EXPECTED_COMPARE_SCHEMA = {
    "mode": "value",
    "warnings": ["value"],
    "comparison_summary": {
        "left_dataset": _EXPECTED_SINGLE_SCHEMA["dataset_summary"],
        "right_dataset": _EXPECTED_SINGLE_SCHEMA["dataset_summary"],
        "row_count_delta": "value",
        "column_matches": {
            "matched": "value",
            "left_only": "value",
            "right_only": "value",
            "total_left": "value",
            "total_right": "value",
            "column_names_left_only": ["value"],
            "column_names_right_only": ["value"],
        },
        "type_mismatches": [
            {
                "column_name": "value",
                "left_type": "value",
                "right_type": "value",
            }
        ],
        "column_profiles_left_only": _EXPECTED_SINGLE_SCHEMA["column_profiles"],
        "column_profiles_right_only": _EXPECTED_SINGLE_SCHEMA["column_profiles"],
    },
    "compare_column_profiles": [
        {
            "column_name": "value",
            "left_profile": _EXPECTED_SINGLE_SCHEMA["column_profiles"][0],
            "right_profile": _EXPECTED_SINGLE_SCHEMA["column_profiles"][0],
            "histograms": [
                {
                    "bin_count": "value",
                    "labels": ["value"],
                    "left_counts": ["value"],
                    "right_counts": ["value"],
                }
            ],
        }
    ],
    "histogram_bins": ["value"],
}


def _sample_dataset_summary(dataset_id: str) -> DatasetSummary:
    return DatasetSummary(
        dataset_id=DatasetId(dataset_id),
        row_count=RowCount(3),
        column_count=ColumnCount(2),
        memory_bytes=MemoryBytes(128),
        missing_cells=MissingCount(0),
        missing_rate=MissingRate(0.0),
        duplicate_rows=DuplicateRowCount(0),
        type_counts=TypeCounts(
            numeric=ColumnCount(1),
            categorical=ColumnCount(1),
            text=ColumnCount(1),
            datetime=ColumnCount(1),
            boolean=ColumnCount(0),
        ),
    )


def _sample_numeric_profile() -> NumericProfile:
    stats = NumericStats(
        minimum=0.0,
        maximum=10.0,
        mean=5.0,
        median=5.0,
        std=1.0,
        variance=1.0,
        quantiles=[QuantileValue(quantile=0.25, value=2.5)],
        iqr=7.5,
    )
    histogram = Histogram(
        bin_count=2,
        bins=[HistogramBin(lower=0.0, upper=5.0, count=1)],
    )
    value_count = NumericValueCount(value=1.0, count=2)
    return NumericProfile(
        is_integer=True,
        stats=stats,
        outlier_rate=OutlierRate(0.0),
        histograms=[histogram],
        top_values=[value_count],
        min_values=[value_count],
        max_values=[value_count],
    )


def _sample_categorical_profile() -> CategoricalProfile:
    histogram = LabeledHistogram(
        bin_count=2,
        labels=["a", "b"],
        counts=[1, 2],
    )
    return CategoricalProfile(
        top_categories=[CategoryCount(category="a", count=1)],
        is_high_cardinality=False,
        suppressed_count=SuppressedCount(0),
        histograms=[histogram],
    )


def _sample_text_profile() -> TextProfile:
    stats = TextLengthStats(mean=3.0, median=3.0, minimum=1, maximum=5)
    histogram = Histogram(
        bin_count=2,
        bins=[HistogramBin(lower=1.0, upper=3.0, count=2)],
    )
    return TextProfile(
        length_stats=stats,
        top_tokens=[TokenCount(token="foo", count=2)],
        length_histograms=[histogram],
    )


def _sample_list_profile() -> ListProfile:
    histogram = Histogram(
        bin_count=2,
        bins=[HistogramBin(lower=1.0, upper=2.0, count=3)],
    )
    return ListProfile(
        length_stats=ListLengthStats(mean=2.0, median=2.0, minimum=1, maximum=3),
        length_histograms=[histogram],
        value_samples=["[1, 2]", "[3]"],
    )


def _sample_datetime_profile() -> DatetimeProfile:
    histogram = LabeledHistogram(
        bin_count=2,
        labels=["2020-01-01", "2020-01-02"],
        counts=[1, 2],
    )
    return DatetimeProfile(
        min_datetime="2020-01-01",
        max_datetime="2020-01-02",
        histograms=[histogram],
        top_values=[DatetimeValueCount(value="2020-01-01", count=1)],
    )


def _sample_column_profiles() -> list[ColumnProfile]:
    base_args = {
        "non_null_count": NonNullCount(3),
        "null_count": NullCount(0),
        "null_rate": NullRate(0.0),
        "unique_count": UniqueCount(3),
        "zero_count": ZeroCount(0),
    }
    return [
        ColumnProfile(
            column_name=ColumnName("numeric"),
            data_type=ColumnType.NUMERIC,
            numeric_profile=_sample_numeric_profile(),
            **base_args,
        ),
        ColumnProfile(
            column_name=ColumnName("categorical"),
            data_type=ColumnType.CATEGORICAL,
            categorical_profile=_sample_categorical_profile(),
            **base_args,
        ),
        ColumnProfile(
            column_name=ColumnName("text"),
            data_type=ColumnType.TEXT,
            text_profile=_sample_text_profile(),
            **base_args,
        ),
        ColumnProfile(
            column_name=ColumnName("datetime"),
            data_type=ColumnType.DATETIME,
            datetime_profile=_sample_datetime_profile(),
            **base_args,
        ),
        ColumnProfile(
            column_name=ColumnName("list"),
            data_type=ColumnType.LIST,
            list_profile=_sample_list_profile(),
            **base_args,
        ),
        ColumnProfile(
            column_name=ColumnName("struct"),
            data_type=ColumnType.STRUCT,
            value_samples=["{'a': 1}", "{'b': 2}"],
            **base_args,
        ),
    ]


def _merge_shapes(left: object, right: object) -> object:
    if isinstance(left, dict) and isinstance(right, dict):
        keys = set(left) | set(right)
        return {key: _merge_shapes(left.get(key), right.get(key)) for key in keys}
    if isinstance(left, list) and isinstance(right, list):
        if not left:
            return right
        if not right:
            return left
        return [_merge_shapes(left[0], right[0])]
    if isinstance(left, dict) and not isinstance(right, dict):
        return left
    if isinstance(right, dict) and not isinstance(left, dict):
        return right
    if isinstance(left, list) and not isinstance(right, list):
        return left
    if isinstance(right, list) and not isinstance(left, list):
        return right
    return left


def _shape(value: object) -> object:
    if isinstance(value, dict):
        return {key: _shape(item) for key, item in value.items()}
    if isinstance(value, list):
        if not value:
            return []
        merged = _shape(value[0])
        for item in value[1:]:
            merged = _merge_shapes(merged, _shape(item))
        return [merged]
    return "value"


def test_single_report_payload_schema() -> None:
    column_profiles = _sample_column_profiles()
    associations = AssociationSummary(
        numeric_numeric=[
            Association(
                left=ColumnName("a"),
                right=ColumnName("b"),
                value=AssociationValue(0.5),
            )
        ],
        categorical_categorical=[
            Association(
                left=ColumnName("c"),
                right=ColumnName("d"),
                value=AssociationValue(0.1),
            )
        ],
        numeric_categorical=[
            Association(
                left=ColumnName("e"),
                right=ColumnName("f"),
                value=AssociationValue(0.2),
            )
        ],
    )

    payload = build_single_report_payload(
        warnings=[WarningMessage("ok")],
        dataset_summary=_sample_dataset_summary("single"),
        column_profiles=column_profiles,
        associations=associations,
        histogram_bins=[10, 20],
    )

    assert _shape(payload) == _EXPECTED_SINGLE_SCHEMA


def test_compare_report_payload_schema() -> None:
    column_profiles = _sample_column_profiles()
    comparison_summary = ComparisonSummary(
        left_dataset=_sample_dataset_summary("left"),
        right_dataset=_sample_dataset_summary("right"),
        row_count_delta=RowCountDelta(0),
        column_matches=ColumnMatchSummary(
            matched=ColumnCount(2),
            left_only=ColumnCount(1),
            right_only=ColumnCount(1),
            total_left=ColumnCount(3),
            total_right=ColumnCount(3),
            column_names_left_only=[ColumnName("left_only")],
            column_names_right_only=[ColumnName("right_only")],
        ),
        type_mismatches=[
            TypeMismatch(
                column_name=ColumnName("mismatch"),
                left_type=ColumnType.NUMERIC,
                right_type=ColumnType.TEXT,
            )
        ],
        column_profiles_left_only=column_profiles,
        column_profiles_right_only=column_profiles,
    )
    compare_profiles = [
        CompareColumnProfile(
            column_name=profile.column_name,
            left_profile=profile,
            right_profile=profile,
            histograms=[
                CompareHistogram(
                    bin_count=2,
                    labels=["low", "high"],
                    left_counts=[1, 2],
                    right_counts=[2, 1],
                )
            ],
        )
        for profile in column_profiles
    ]

    payload = build_compare_report_payload(
        warnings=[WarningMessage("ok")],
        comparison_summary=comparison_summary,
        compare_column_profiles=compare_profiles,
        histogram_bins=[10, 20],
    )

    assert _shape(payload) == _EXPECTED_COMPARE_SCHEMA
