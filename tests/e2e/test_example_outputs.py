from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.example_reports import build_example_reports

pytestmark = pytest.mark.e2e


def test_example_outputs_match_current_reports() -> None:
    output_dir = Path(__file__).resolve().parents[2] / "examples" / "output"
    expected_single_path = output_dir / "single_report.html"
    expected_compare_path = output_dir / "compare_report.html"
    expected_single = (
        expected_single_path.read_text(encoding="utf-8")
        if expected_single_path.exists()
        else ""
    )
    expected_compare = (
        expected_compare_path.read_text(encoding="utf-8")
        if expected_compare_path.exists()
        else ""
    )

    single_html, compare_html = build_example_reports(output_dir)

    assert single_html == expected_single
    assert compare_html == expected_compare
