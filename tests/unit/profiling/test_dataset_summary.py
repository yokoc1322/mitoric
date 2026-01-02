from __future__ import annotations

import datetime as dt

import polars as pl

from mitoric.profiling.dataset import summarize_dataset


def test_dataset_summary_counts() -> None:
    frame = pl.DataFrame(
        {
            "num": [1, 2, None, 1],
            "cat": ["A", "B", "A", "A"],
            "flag": [True, False, True, True],
            "when": [
                dt.date(2024, 1, 1),
                dt.date(2024, 1, 2),
                dt.date(2024, 1, 3),
                dt.date(2024, 1, 1),
            ],
        }
    )

    summary = summarize_dataset(frame, dataset_id="main")

    assert summary.row_count == 4
    assert summary.column_count == 4
    assert summary.missing_cells == 1
    assert summary.duplicate_rows == 1
    assert summary.type_counts.numeric == 1
    assert summary.type_counts.categorical == 1
    assert summary.type_counts.text == 0
    assert summary.type_counts.datetime == 1
    assert summary.type_counts.boolean == 1
    assert 0.0 < summary.missing_rate <= 1.0
    assert summary.memory_bytes > 0


def test_dataset_summary_empty() -> None:
    frame = pl.DataFrame()

    summary = summarize_dataset(frame, dataset_id="empty")

    assert summary.row_count == 0
    assert summary.column_count == 0
    assert summary.missing_cells == 0
    assert summary.missing_rate == 0.0
    assert summary.duplicate_rows == 0
    assert summary.type_counts.numeric == 0
    assert summary.type_counts.categorical == 0
    assert summary.type_counts.text == 0
    assert summary.type_counts.datetime == 0
    assert summary.type_counts.boolean == 0
