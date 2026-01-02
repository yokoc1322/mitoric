"""Report payload builders."""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import asdict
from typing import cast

from mitoric.models.aggregation import (
    AssociationSummary,
    ColumnProfile,
    CompareColumnProfile,
    ComparisonSummary,
    DatasetSummary,
)
from mitoric.models.base import WarningMessage
from mitoric.reporting.payload_schema import (
    AssociationSummaryPayload,
    ColumnProfilePayload,
    CompareColumnProfilePayload,
    CompareReportPayload,
    ComparisonSummaryPayload,
    DatasetSummaryPayload,
    SingleReportPayload,
)


def build_single_report_payload(
    *,
    warnings: list[WarningMessage],
    dataset_summary: DatasetSummary,
    column_profiles: list[ColumnProfile],
    associations: AssociationSummary,
    histogram_bins: Sequence[int],
) -> SingleReportPayload:
    dataset_payload = cast(DatasetSummaryPayload, asdict(dataset_summary))
    column_payloads = [
        cast(ColumnProfilePayload, asdict(profile)) for profile in column_profiles
    ]
    associations_payload = cast(AssociationSummaryPayload, asdict(associations))
    return {
        "mode": "single",
        "warnings": warnings,
        "dataset_summary": dataset_payload,
        "column_profiles": column_payloads,
        "associations": associations_payload,
        "histogram_bins": histogram_bins,
    }


def build_compare_report_payload(
    *,
    warnings: list[WarningMessage],
    comparison_summary: ComparisonSummary,
    compare_column_profiles: list[CompareColumnProfile],
    histogram_bins: Sequence[int],
) -> CompareReportPayload:
    comparison_payload = cast(ComparisonSummaryPayload, asdict(comparison_summary))
    compare_payloads = [
        cast(CompareColumnProfilePayload, asdict(profile))
        for profile in compare_column_profiles
    ]
    return {
        "mode": "compare",
        "warnings": warnings,
        "comparison_summary": comparison_payload,
        "compare_column_profiles": compare_payloads,
        "histogram_bins": histogram_bins,
    }
