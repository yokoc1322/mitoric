from __future__ import annotations

import polars as pl

from mitoric.models.base import ColumnName
from mitoric.profiling.columns import compare_column_profiles


def test_compare_profiles_left_right_only() -> None:
    left = pl.DataFrame({"shared": [1, 2], "left_only": [10, 20]})
    right = pl.DataFrame({"shared": [1, 2], "right_only": ["A", "B"]})

    left_only, right_only = compare_column_profiles(left, right)

    assert [profile.column_name for profile in left_only] == [ColumnName("left_only")]
    assert [profile.column_name for profile in right_only] == [ColumnName("right_only")]
