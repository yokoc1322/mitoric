from __future__ import annotations

from pathlib import Path

import pytest

from tests.e2e.example_reports import build_example_reports

pytestmark = pytest.mark.e2e


def test_example_outputs_are_generated(tmp_path: Path) -> None:
    output_dir = tmp_path / "output"

    single_html, compare_html = build_example_reports(output_dir)

    single_output = output_dir / "single_report.html"
    compare_output = output_dir / "compare_report.html"

    assert single_output.exists()
    assert compare_output.exists()
    assert single_output.read_text(encoding="utf-8") == single_html
    assert compare_output.read_text(encoding="utf-8") == compare_html
    assert single_html
    assert compare_html
