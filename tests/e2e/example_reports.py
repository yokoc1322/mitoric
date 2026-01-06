from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import polars as pl

from mitoric import generate_compare_report, generate_single_report

_logger = logging.getLogger(__name__)
_REFERENCE_DATE = dt.datetime(1912, 4, 14)
_REFERENCE_TIME = dt.time(12, 0)


def _enrich_example_frame(frame: pl.DataFrame) -> pl.DataFrame:
    additional_columns = [
        pl.lit(_REFERENCE_TIME).alias("noon_time"),
        pl.lit(None, dtype=pl.Null).alias("always_null"),
    ]

    if "survived" in frame.columns:
        additional_columns.append(pl.col("survived").cast(pl.Boolean).alias("survived"))

    if "age" in frame.columns:
        additional_columns.append(
            (pl.lit(_REFERENCE_DATE) - pl.duration(days=pl.col("age") * 365.25)).alias(
                "birthdate"
            )
        )
        additional_columns.append(
            pl.duration(days=pl.col("age").fill_null(0).cast(pl.Int64)).alias(
                "age_duration"
            )
        )

    if "name" in frame.columns:
        additional_columns.append(
            pl.col("name")
            .map_elements(
                lambda value: value.encode("utf-8") if value is not None else None,
                return_dtype=pl.Binary,
            )
            .alias("name_binary")
        )

    if "sex" in frame.columns:
        additional_columns.append(
            pl.when(pl.col("sex") == "female")
            .then(pl.lit("f"))
            .otherwise(pl.lit("m"))
            .cast(pl.Enum(["f", "m"]))
            .alias("sex_enum")
        )

    if "embarked" in frame.columns:
        additional_columns.append(
            pl.col("embarked").cast(pl.Categorical).alias("embarked_category")
        )

    if {"sibsp", "parch", "fare"}.issubset(frame.columns):
        additional_columns.append(
            pl.struct(
                siblings_spouses=pl.col("sibsp"),
                parents_children=pl.col("parch"),
                fare=pl.col("fare"),
            ).alias("family_struct")
        )

    voyage_notes_expr: pl.Expr | None = None
    if {"sibsp", "parch", "survived"}.issubset(frame.columns):
        voyage_notes_expr = pl.concat_list(
            [pl.col("sibsp"), pl.col("parch"), pl.col("survived")]
        ).alias("voyage_notes")
        additional_columns.append(voyage_notes_expr)

    enriched = frame.with_columns(additional_columns)
    if voyage_notes_expr is not None:
        enriched = enriched.with_columns(
            pl.col("voyage_notes").list.to_array(width=3).alias("voyage_array")
        )
    return enriched


def _split_frame_by_survived(frame: pl.DataFrame) -> tuple[pl.DataFrame, pl.DataFrame]:
    survived_frame = frame.filter(pl.col("survived") == True)
    not_survived_frame = frame.filter(pl.col("survived") == False)
    return survived_frame, not_survived_frame


def _load_example_frames() -> tuple[pl.DataFrame, pl.DataFrame, pl.DataFrame]:
    dataset_path = (
        Path(__file__).resolve().parents[2] / "tests" / "e2e" / "data" / "titanic3.csv"
    )
    frame = _enrich_example_frame(pl.read_csv(dataset_path))
    left, right = _split_frame_by_survived(frame)
    return frame, left, right


def _write_report(output_path: Path, html: str) -> None:
    output_path.write_text(html, encoding="utf-8")
    _logger.info(
        "saved report: %s (size=%s bytes)", output_path, output_path.stat().st_size
    )


def build_example_reports(output_dir: Path) -> tuple[str, str]:
    output_dir.mkdir(exist_ok=True)

    frame, left, right = _load_example_frames()

    single_html = generate_single_report(frame)
    compare_html = generate_compare_report(left, right)

    _write_report(output_dir / "single_report.html", single_html)
    _write_report(output_dir / "compare_report.html", compare_html)

    return single_html, compare_html
