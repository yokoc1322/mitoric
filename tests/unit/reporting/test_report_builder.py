from __future__ import annotations

from dataclasses import asdict

from mitoric.models.aggregation import (
    Association,
    AssociationSummary,
    ColumnMatchSummary,
    ColumnProfile,
    CompareColumnProfile,
    ComparisonSummary,
    DatasetSummary,
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
    RowCount,
    RowCountDelta,
    UniqueCount,
    WarningMessage,
    ZeroCount,
)
from mitoric.reporting.builder import (
    build_compare_report_payload,
    build_single_report_payload,
)


def _sample_column_profile(name: str) -> ColumnProfile:
    return ColumnProfile(
        column_name=ColumnName(name),
        data_type=ColumnType.NUMERIC,
        non_null_count=NonNullCount(3),
        null_count=NullCount(0),
        null_rate=NullRate(0.0),
        unique_count=UniqueCount(3),
        zero_count=ZeroCount(0),
    )


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
            text=ColumnCount(0),
            datetime=ColumnCount(0),
            boolean=ColumnCount(0),
        ),
    )


def test_build_single_report_payload() -> None:
    warnings = [WarningMessage("ok")]
    dataset_summary = _sample_dataset_summary("single")
    column_profile = _sample_column_profile("age")
    associations = AssociationSummary(
        numeric_numeric=[
            Association(
                left=ColumnName("age"),
                right=ColumnName("height"),
                value=AssociationValue(0.5),
            )
        ],
        categorical_categorical=[],
        numeric_categorical=[],
    )
    histogram_bins = [10, 20]

    payload = build_single_report_payload(
        warnings=warnings,
        dataset_summary=dataset_summary,
        column_profiles=[column_profile],
        associations=associations,
        histogram_bins=histogram_bins,
    )

    assert payload == {
        "mode": "single",
        "warnings": warnings,
        "dataset_summary": asdict(dataset_summary),
        "column_profiles": [asdict(column_profile)],
        "associations": asdict(associations),
        "histogram_bins": histogram_bins,
    }


def test_build_compare_report_payload() -> None:
    warnings = [WarningMessage("ok")]
    left_profile = _sample_column_profile("age")
    right_profile = _sample_column_profile("age")
    comparison_summary = ComparisonSummary(
        left_dataset=_sample_dataset_summary("left"),
        right_dataset=_sample_dataset_summary("right"),
        row_count_delta=RowCountDelta(0),
        column_matches=ColumnMatchSummary(
            matched=ColumnCount(1),
            left_only=ColumnCount(0),
            right_only=ColumnCount(0),
            total_left=ColumnCount(1),
            total_right=ColumnCount(1),
            column_names_left_only=[],
            column_names_right_only=[],
        ),
        type_mismatches=[
            TypeMismatch(
                column_name=ColumnName("age"),
                left_type=ColumnType.NUMERIC,
                right_type=ColumnType.NUMERIC,
            )
        ],
        column_profiles_left_only=[left_profile],
        column_profiles_right_only=[right_profile],
    )
    compare_profile = CompareColumnProfile(
        column_name=ColumnName("age"),
        left_profile=left_profile,
        right_profile=right_profile,
    )
    histogram_bins = [10, 20]

    payload = build_compare_report_payload(
        warnings=warnings,
        comparison_summary=comparison_summary,
        compare_column_profiles=[compare_profile],
        histogram_bins=histogram_bins,
    )

    assert payload == {
        "mode": "compare",
        "warnings": warnings,
        "comparison_summary": asdict(comparison_summary),
        "compare_column_profiles": [asdict(compare_profile)],
        "histogram_bins": histogram_bins,
    }
