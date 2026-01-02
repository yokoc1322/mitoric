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
