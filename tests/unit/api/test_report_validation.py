from __future__ import annotations

from typing import cast

import polars as pl
import pytest

from mitoric import generate_compare_report, generate_single_report
from mitoric.models.base import ColumnName, ColumnType, ExplicitType


def test_generate_single_report_rejects_language_parameter() -> None:
    frame = pl.DataFrame({"value": [1, 2, 3]})

    with pytest.raises(TypeError, match="language"):
        generate_single_report(frame, language="en")  # type: ignore[call-arg]


def test_generate_compare_report_rejects_language_parameter() -> None:
    left = pl.DataFrame({"value": [1, 2, 3]})
    right = pl.DataFrame({"value": [4, 5, 6]})

    with pytest.raises(TypeError, match="language"):
        generate_compare_report(left, right, language="en")  # type: ignore[call-arg]


def test_generate_single_report_invalid_save_path() -> None:
    frame = pl.DataFrame({"value": [1, 2, 3]})

    with pytest.raises(ValueError, match="save_path must be a non-empty string"):
        generate_single_report(frame, save_path="   ")


def test_generate_single_report_writes_file(tmp_path) -> None:
    frame = pl.DataFrame({"value": [1, 2, 3]})
    output_path = tmp_path / "report.html"

    html = generate_single_report(frame, save_path=str(output_path))

    assert output_path.exists()
    assert "Overview" in html
    assert "Overview" in output_path.read_text(encoding="utf-8")


def test_generate_single_report_missing_target_columns() -> None:
    frame = pl.DataFrame({"value": [1, 2, 3]})

    with pytest.raises(ValueError, match="target_columns not found"):
        generate_single_report(frame, target_columns=["missing"])


def test_generate_single_report_invalid_explicit_type_name() -> None:
    frame = pl.DataFrame({"value": [1, 2, 3]})

    with pytest.raises(ValueError, match="explicit_types contains unknown columns"):
        generate_single_report(
            frame,
            explicit_types=[
                ExplicitType(ColumnName("value"), cast(ColumnType, "unknown"))
            ],
        )


def test_generate_single_report_invalid_explicit_type_column() -> None:
    frame = pl.DataFrame({"value": [1, 2, 3]})

    with pytest.raises(ValueError, match="explicit_types contains unknown columns"):
        generate_single_report(
            frame,
            explicit_types=[ExplicitType(ColumnName("missing"), ColumnType.NUMERIC)],
        )


def test_generate_single_report_empty_frame_warning() -> None:
    frame = pl.DataFrame()

    html = generate_single_report(frame)

    assert "Input DataFrame is empty" in html


def test_generate_compare_report_missing_target_columns() -> None:
    left = pl.DataFrame({"value": [1, 2, 3]})
    right = pl.DataFrame({"value": [1, 2, 3]})

    with pytest.raises(ValueError, match="target_columns not found"):
        generate_compare_report(left, right, target_columns=["missing"])
