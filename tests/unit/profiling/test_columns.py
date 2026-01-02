from __future__ import annotations

import datetime as dt

import polars as pl

from mitoric.models.base import ColumnName, ColumnType, ExplicitType
from mitoric.profiling.columns import profile_columns


def test_column_profiles_by_type() -> None:
    size = 101
    frame = pl.DataFrame(
        {
            "numeric": [0, 1, 2] + [3] * (size - 3),
            "categorical": ["A", "B", "A"] + ["A"] * (size - 3),
            "textual": [f"token_{i}" for i in range(size)],
            "when": [dt.date(2024, 1, 1)] * size,
        }
    )

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}

    numeric = profile_map[ColumnName("numeric")]
    assert numeric.data_type == ColumnType.NUMERIC
    assert numeric.numeric_profile is not None
    assert numeric.zero_count == 1

    categorical = profile_map[ColumnName("categorical")]
    assert categorical.data_type == ColumnType.CATEGORICAL
    assert categorical.categorical_profile is not None
    assert categorical.categorical_profile.is_high_cardinality is False

    textual = profile_map[ColumnName("textual")]
    assert textual.data_type == ColumnType.TEXT
    assert textual.text_profile is not None

    when = profile_map[ColumnName("when")]
    assert when.data_type == ColumnType.DATETIME
    assert when.datetime_profile is not None


def test_target_columns_filtering() -> None:
    frame = pl.DataFrame(
        {
            "numeric": [1, 2, 3],
            "categorical": ["A", "B", "A"],
        }
    )

    profiles = profile_columns(
        frame,
        target_columns=["numeric"],
        explicit_types=[ExplicitType(ColumnName("categorical"), ColumnType.TEXT)],
    )
    profile_map = {profile.column_name: profile for profile in profiles}

    numeric = profile_map[ColumnName("numeric")]
    assert numeric.numeric_profile is not None

    categorical = profile_map[ColumnName("categorical")]
    assert categorical.categorical_profile is None
    assert categorical.text_profile is None
