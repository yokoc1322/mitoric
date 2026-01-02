from __future__ import annotations

import json
import re

import polars as pl

from mitoric import generate_single_report


def test_single_report_html_and_no_save(tmp_path) -> None:
    frame = pl.DataFrame(
        {
            "age": [10, 12, 12, 14],
            "city": ["A", "B", "A", "C"],
        }
    )

    output_path = tmp_path / "report.html"
    html = generate_single_report(frame)

    assert "Overview" in html
    assert "Dataset Summary" in html
    assert "Variables" in html
    assert "Associations" in html
    assert not output_path.exists()
    assert "Missing cells" not in html


def test_single_report_does_not_embed_raw_data() -> None:
    frame = pl.DataFrame(
        {
            "age": [10, 12, 12, 14],
            "secret": ["SECRET_VALUE", "SECRET_VALUE", "SECRET_VALUE", "SECRET_VALUE"],
        }
    )

    html = generate_single_report(frame, target_columns=["age"])

    assert "SECRET_VALUE" not in html


def test_single_report_includes_histogram_chart() -> None:
    frame = pl.DataFrame({"value": [1, 2, 2, 3, 4]})

    html = generate_single_report(frame)

    assert "js-histogram-chart-container" in html
    assert "js-histogram-canvas" in html
    assert "js-histogram-data" in html
    assert "chart.umd.min.js" in html


def test_single_report_histogram_json_is_valid() -> None:
    frame = pl.DataFrame({"category": ['alpha"', "line\nbreak", 'alpha"']})

    html = generate_single_report(frame)

    matches = re.findall(
        r'<script type="application/json" class="js-histogram-data">\s*(.*?)\s*</script>',
        html,
        re.DOTALL,
    )
    assert matches
    for payload in matches:
        json.loads(payload)


def test_single_report_includes_text_top_values() -> None:
    values = [f"token_{idx}" for idx in range(105)]
    values.extend(["alpha", "alpha"])
    frame = pl.DataFrame({"note": values})

    html = generate_single_report(frame)

    assert "Most frequent values" in html
    assert "alpha" in html
