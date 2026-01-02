from __future__ import annotations

import polars as pl

from mitoric import generate_compare_report, generate_single_report


def test_single_report_outputs_html_file(tmp_path) -> None:
    frame = pl.DataFrame(
        {
            "age": [10, 12, 12, 14],
            "city": ["A", "B", "A", "C"],
        }
    )

    output_path = tmp_path / "single_report.html"

    generate_single_report(frame, save_path=str(output_path))

    assert output_path.exists()
    assert output_path.stat().st_size > 0


def test_compare_report_outputs_html_file(tmp_path) -> None:
    left = pl.DataFrame({"id": [1, 2], "value": [10, 20]})
    right = pl.DataFrame({"id": [1, 2], "value": [10, 25]})

    output_path = tmp_path / "compare_report.html"

    generate_compare_report(left, right, save_path=str(output_path))

    assert output_path.exists()
    assert output_path.stat().st_size > 0
