from __future__ import annotations

import datetime as dt

import polars as pl

from mitoric.models.base import ColumnType
from mitoric.profiling.utils.type_utils import classify_column_type, infer_column_type


def test_infer_column_type_for_basic_dtypes() -> None:
    assert infer_column_type(pl.Series("flag", [True, False])) == ColumnType.BOOLEAN
    assert (
        infer_column_type(pl.Series("ts", [dt.datetime(2024, 1, 1)]))
        == ColumnType.DATETIME
    )
    assert infer_column_type(pl.Series("value", [1, 2, 3])) == ColumnType.NUMERIC
    assert infer_column_type(pl.Series("cat", ["a", "b"])) == ColumnType.CATEGORICAL


def test_classify_column_type_for_text_threshold() -> None:
    values = [f"value-{index}" for index in range(101)]
    series = pl.Series("text", values)
    assert classify_column_type(series) == ColumnType.TEXT


def test_infer_column_type_for_extended_polars_dtypes() -> None:
    assert (
        infer_column_type(pl.Series("time", [dt.time(1, 2)], dtype=pl.Time))
        == ColumnType.DATETIME
    )
    duration = pl.Series("duration", [dt.timedelta(days=1)], dtype=pl.Duration)
    assert infer_column_type(duration) == ColumnType.NUMERIC
    binary_series = pl.Series("binary", [b"a", b"bc"], dtype=pl.Binary)
    assert infer_column_type(binary_series) == ColumnType.NUMERIC
    array_series = pl.Series("array", [[1, 2, 3]], dtype=pl.Array(pl.Int64, 3))
    assert infer_column_type(array_series) == ColumnType.LIST
    enum_series = pl.Series("enum", ["x", "y"], dtype=pl.Enum(["x", "y"]))
    assert infer_column_type(enum_series) == ColumnType.CATEGORICAL
