from __future__ import annotations

import datetime as dt
import logging
from pathlib import Path

import polars as pl

from mitoric import generate_compare_report, generate_single_report

_logger = logging.getLogger(__name__)
_REFERENCE_DATE = dt.datetime(1912, 4, 14)


def _enrich_example_frame(frame: pl.DataFrame) -> pl.DataFrame:
    additional_columns = [
        pl.col("survived").cast(pl.Boolean).alias("survived"),
        (pl.lit(_REFERENCE_DATE) - pl.duration(days=pl.col("age") * 365.25)).alias(
            "birthdate"
        ),
    ]

    if {"sibsp", "parch", "fare"}.issubset(frame.columns):
        additional_columns.append(
            pl.struct(
                siblings_spouses=pl.col("sibsp"),
                parents_children=pl.col("parch"),
                fare=pl.col("fare"),
            ).alias("family_struct")
        )

    if {"sibsp", "parch", "survived"}.issubset(frame.columns):
        additional_columns.append(
            pl.concat_list(
                [pl.col("sibsp"), pl.col("parch"), pl.col("survived")]
            ).alias("voyage_notes")
        )

    return frame.with_columns(additional_columns)


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
