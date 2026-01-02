from __future__ import annotations

import polars as pl

from mitoric import generate_compare_report


def test_compare_report_html_sections() -> None:
    left = pl.DataFrame({"id": [1, 2], "value": [10, 20]})
    right = pl.DataFrame({"id": [1, 2], "value": [10, 25]})

    html = generate_compare_report(left, right)

    assert "Overview" in html
    assert "Dataset Summary" in html
    assert "Differences" not in html


def test_compare_report_does_not_embed_raw_data() -> None:
    left = pl.DataFrame({"id": [1, 2], "secret": ["SECRET", "SECRET"]})
    right = pl.DataFrame({"id": [1, 2], "secret": ["SECRET", "SECRET"]})

    html = generate_compare_report(left, right, target_columns=["id"])

    assert "SECRET" not in html


def test_compare_report_includes_variable_details() -> None:
    left = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 20, 30],
            "left_only": [1, 1, 2],
        }
    )
    right = pl.DataFrame(
        {
            "id": [1, 2, 3],
            "value": [10, 25, 40],
            "right_only": [3, 3, 4],
        }
    )

    html = generate_compare_report(left, right)

    assert 'data-column-name="value"' in html
    assert 'data-column-name="left_only"' in html
    assert 'data-column-name="right_only"' in html
    assert "left_counts" in html
    assert "right_counts" in html


def test_compare_report_hides_associations_section() -> None:
    left = pl.DataFrame({"id": [1, 2], "value": [10, 20]})
    right = pl.DataFrame({"id": [1, 2], "value": [10, 25]})

    html = generate_compare_report(left, right)

    assert 'data-section="associations"' not in html
    assert 'data-target="associations"' not in html
    assert "Associations" not in html


def test_compare_report_hides_differences_section() -> None:
    left = pl.DataFrame({"id": [1, 2], "value": [10, 20]})
    right = pl.DataFrame({"id": [1, 2], "value": [10, 25]})

    html = generate_compare_report(left, right)

    assert 'data-section="differences"' not in html
    assert 'data-target="differences"' not in html


def test_compare_report_uses_custom_labels() -> None:
    left = pl.DataFrame({"id": [1, 2], "value": [10, 20]})
    right = pl.DataFrame({"id": [1, 2], "value": [10, 25]})

    html = generate_compare_report(left, right, left_name="Train", right_name="Test")

    assert 'data-left-label="Train"' in html
    assert 'data-right-label="Test"' in html
    assert "Train composition" in html
    assert "Test composition" in html
