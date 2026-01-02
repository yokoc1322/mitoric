from __future__ import annotations

import polars as pl

from mitoric.models.base import ColumnName
from mitoric.profiling.dataset import summarize_comparison


def test_comparison_summary_basic() -> None:
    left = pl.DataFrame({"id": [1, 2, 3], "left_only": [10, 20, 30]})
    right = pl.DataFrame({"id": ["1", "2"], "right_only": [1, 2]})

    summary = summarize_comparison(left, right, left_id="left", right_id="right")

    assert summary.row_count_delta == 1
    assert summary.column_matches.matched == 1
    assert summary.column_matches.left_only == 1
    assert summary.column_matches.right_only == 1
    assert ColumnName("left_only") in summary.column_matches.column_names_left_only
    assert ColumnName("right_only") in summary.column_matches.column_names_right_only
    assert summary.type_mismatches[0].column_name == ColumnName("id")
