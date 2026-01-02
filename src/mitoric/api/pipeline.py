"""Report request normalization and pipeline execution."""

from __future__ import annotations

import logging
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

import polars as pl

from mitoric.models.aggregation import ComparisonSummary
from mitoric.models.base import (
    ColumnName,
    ColumnType,
    DatasetId,
    ExplicitType,
    SavePath,
    WarningMessage,
)
from mitoric.profiling.associations import compute_associations
from mitoric.profiling.columns import (
    compare_column_profiles,
    compare_common_column_profiles,
    profile_columns,
)
from mitoric.profiling.dataset import summarize_comparison, summarize_dataset
from mitoric.profiling.histograms.config import HISTOGRAM_BINS
from mitoric.render.template import render_report
from mitoric.reporting.builder import (
    build_compare_report_payload,
    build_single_report_payload,
)

_logger = logging.getLogger(__name__)


def _normalize_target_columns(target_columns: list[str] | None) -> list[ColumnName]:
    if not target_columns:
        return []
    return [ColumnName(name) for name in target_columns]


def _normalize_explicit_types(
    explicit_types: list[ExplicitType] | None,
) -> list[ExplicitType]:
    if not explicit_types:
        return []
    return list(explicit_types)


def _normalize_save_path(save_path: str | None) -> SavePath:
    if save_path is None:
        return SavePath("")
    if not save_path.strip():
        raise ValueError("save_path must be a non-empty string when provided")
    return SavePath(save_path)


def _normalize_compare_label(label: str | None, default: str) -> DatasetId:
    if label is None:
        return DatasetId(default)
    cleaned = label.strip()
    return DatasetId(cleaned or default)


def _validate_target_columns(
    frame: pl.DataFrame, target_columns: list[ColumnName]
) -> None:
    if not target_columns:
        return
    column_names = set(frame.columns)
    missing = [name for name in target_columns if name not in column_names]
    if missing:
        raise ValueError(f"target_columns not found in DataFrame: {missing}")


def _validate_explicit_types(
    frame: pl.DataFrame, explicit_types: list[ExplicitType]
) -> list[ExplicitType]:
    if not explicit_types:
        return []
    column_names = set(frame.columns)
    normalized: list[ExplicitType] = []
    invalid = False
    for item in explicit_types:
        try:
            data_type = ColumnType.from_raw(item.data_type)
        except ValueError:
            invalid = True
            continue
        if item.column_name not in column_names:
            invalid = True
            continue
        normalized.append(ExplicitType(item.column_name, data_type))
    if invalid:
        raise ValueError("explicit_types contains unknown columns or types")
    return normalized


def _collect_input_warnings(frame: pl.DataFrame) -> list[WarningMessage]:
    warnings: list[WarningMessage] = []
    if frame.height == 0 or frame.width == 0:
        warnings.append(
            WarningMessage("Input DataFrame is empty; report will contain no data.")
        )
    return warnings


def _log_info_start(name: str) -> float:
    _logger.info("%s: start", name)
    return time.perf_counter()


def _log_info_end(name: str, start: float) -> None:
    elapsed = time.perf_counter() - start
    _logger.info("%s: end (elapsed=%.4fs)", name, elapsed)


def _log_debug_counts(label: str, frame: pl.DataFrame) -> None:
    _logger.debug("%s: rows=%s cols=%s", label, frame.height, frame.width)


def _default_template_path() -> Path:
    return Path(__file__).resolve().parents[1] / "templates" / "report.html"


def _optional_target_columns(target_columns: list[ColumnName]) -> list[str] | None:
    if not target_columns:
        return None
    return [str(name) for name in target_columns]


@dataclass(frozen=True)
class SingleReportRequest:
    frame: pl.DataFrame
    target_columns: list[ColumnName]
    explicit_types: list[ExplicitType]
    save_path: SavePath

    @classmethod
    def from_raw(
        cls,
        frame: pl.DataFrame,
        *,
        target_columns: list[str] | None,
        explicit_types: list[ExplicitType] | None,
        save_path: str | None,
    ) -> SingleReportRequest:
        normalized_target_columns = _normalize_target_columns(target_columns)
        normalized_explicit_types = _normalize_explicit_types(explicit_types)
        normalized_save_path = _normalize_save_path(save_path)
        _validate_target_columns(frame, normalized_target_columns)
        validated_explicit_types = _validate_explicit_types(
            frame, normalized_explicit_types
        )
        return cls(
            frame=frame,
            target_columns=normalized_target_columns,
            explicit_types=validated_explicit_types,
            save_path=normalized_save_path,
        )


@dataclass(frozen=True)
class CompareReportRequest:
    left: pl.DataFrame
    right: pl.DataFrame
    target_columns: list[ColumnName]
    explicit_types: list[ExplicitType]
    save_path: SavePath
    left_name: DatasetId
    right_name: DatasetId

    @classmethod
    def from_raw(
        cls,
        left: pl.DataFrame,
        right: pl.DataFrame,
        *,
        target_columns: list[str] | None,
        explicit_types: list[ExplicitType] | None,
        save_path: str | None,
        left_name: str | None,
        right_name: str | None,
    ) -> CompareReportRequest:
        normalized_target_columns = _normalize_target_columns(target_columns)
        normalized_explicit_types = _normalize_explicit_types(explicit_types)
        normalized_save_path = _normalize_save_path(save_path)
        normalized_left_name = _normalize_compare_label(left_name, "left")
        normalized_right_name = _normalize_compare_label(right_name, "right")
        _validate_target_columns(left, normalized_target_columns)
        _validate_target_columns(right, normalized_target_columns)
        validated_explicit_types = _validate_explicit_types(
            left, normalized_explicit_types
        )
        _validate_explicit_types(right, validated_explicit_types)
        return cls(
            left=left,
            right=right,
            target_columns=normalized_target_columns,
            explicit_types=validated_explicit_types,
            save_path=normalized_save_path,
            left_name=normalized_left_name,
            right_name=normalized_right_name,
        )


class ReportPipeline:
    def __init__(
        self,
        *,
        template_path: Path | None = None,
        histogram_bins: Sequence[int] = HISTOGRAM_BINS,
    ) -> None:
        self._template_path = template_path or _default_template_path()
        self._histogram_bins = histogram_bins

    def generate_single(self, request: SingleReportRequest) -> str:
        start = _log_info_start("generate_single_report")
        _log_debug_counts("input", request.frame)

        warnings = _collect_input_warnings(request.frame)
        for warning in warnings:
            _logger.warning("generate_single_report: %s", warning)

        dataset_summary = summarize_dataset(request.frame, dataset_id="single")
        column_profiles = profile_columns(
            request.frame,
            target_columns=_optional_target_columns(request.target_columns),
            explicit_types=request.explicit_types,
        )
        associations = compute_associations(request.frame)
        payload = build_single_report_payload(
            warnings=warnings,
            dataset_summary=dataset_summary,
            column_profiles=column_profiles,
            associations=associations,
            histogram_bins=self._histogram_bins,
        )
        html = render_report(self._template_path, payload)
        if request.save_path:
            Path(request.save_path).write_text(html, encoding="utf-8")

        _log_info_end("generate_single_report", start)
        return html

    def generate_compare(self, request: CompareReportRequest) -> str:
        start = _log_info_start("generate_compare_report")
        _log_debug_counts("left", request.left)
        _log_debug_counts("right", request.right)

        warnings = _collect_input_warnings(request.left) + _collect_input_warnings(
            request.right
        )
        for warning in warnings:
            _logger.warning("generate_compare_report: %s", warning)

        base_summary = summarize_comparison(
            request.left,
            request.right,
            left_id=request.left_name,
            right_id=request.right_name,
        )
        target_columns = _optional_target_columns(request.target_columns)
        left_only_profiles, right_only_profiles = compare_column_profiles(
            request.left,
            request.right,
            target_columns=target_columns,
            explicit_types=request.explicit_types,
        )
        compare_profiles = compare_common_column_profiles(
            request.left,
            request.right,
            target_columns=target_columns,
            explicit_types=request.explicit_types,
        )
        comparison_summary = ComparisonSummary(
            left_dataset=base_summary.left_dataset,
            right_dataset=base_summary.right_dataset,
            row_count_delta=base_summary.row_count_delta,
            column_matches=base_summary.column_matches,
            type_mismatches=base_summary.type_mismatches,
            column_profiles_left_only=left_only_profiles,
            column_profiles_right_only=right_only_profiles,
        )
        payload = build_compare_report_payload(
            warnings=warnings,
            comparison_summary=comparison_summary,
            compare_column_profiles=compare_profiles,
            histogram_bins=self._histogram_bins,
        )
        html = render_report(self._template_path, payload)
        if request.save_path:
            Path(request.save_path).write_text(html, encoding="utf-8")

        _log_info_end("generate_compare_report", start)
        return html
