"""Report-facing types and options."""

from __future__ import annotations

from dataclasses import dataclass, field

from mitoric.models.aggregation import (
    AssociationSummary,
    ColumnProfile,
    ComparisonSummary,
    DatasetSummary,
)
from mitoric.models.base import (
    ColumnName,
    ExplicitType,
    GeneratedAt,
    HtmlString,
    ReportId,
    ReportMode,
    SavePath,
    WarningMessage,
)


@dataclass(frozen=True)
class ReportOptions:
    target_columns: list[ColumnName] = field(default_factory=list)
    explicit_types: list[ExplicitType] = field(default_factory=list)
    save_path: SavePath = SavePath("")


@dataclass(frozen=True)
class Report:
    report_id: ReportId
    mode: ReportMode
    generated_at: GeneratedAt
    html: HtmlString
    warnings: list[WarningMessage]
    dataset_summary: DatasetSummary | None = None
    comparison_summary: ComparisonSummary | None = None
    column_profiles: list[ColumnProfile] = field(default_factory=list)
    association_summary: AssociationSummary | None = None
