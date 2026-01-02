"""Report generation entry points."""

from __future__ import annotations

import polars as pl

from mitoric.api.pipeline import (
    CompareReportRequest,
    ReportPipeline,
    SingleReportRequest,
)
from mitoric.models.base import ExplicitType


def generate_single_report(
    frame: pl.DataFrame,
    *,
    target_columns: list[str] | None = None,
    explicit_types: list[ExplicitType] | None = None,
    save_path: str | None = None,
) -> str:
    request = SingleReportRequest.from_raw(
        frame,
        target_columns=target_columns,
        explicit_types=explicit_types,
        save_path=save_path,
    )
    return ReportPipeline().generate_single(request)


def generate_compare_report(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    target_columns: list[str] | None = None,
    explicit_types: list[ExplicitType] | None = None,
    save_path: str | None = None,
    left_name: str | None = None,
    right_name: str | None = None,
) -> str:
    request = CompareReportRequest.from_raw(
        left,
        right,
        target_columns=target_columns,
        explicit_types=explicit_types,
        save_path=save_path,
        left_name=left_name,
        right_name=right_name,
    )
    return ReportPipeline().generate_compare(request)
