"""Column dtype helpers."""

from __future__ import annotations

import polars as pl

from mitoric.models.base import ColumnType

TEXT_CARDINALITY_THRESHOLD = 100
_NUMERIC_DTYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
    pl.Float32,
    pl.Float64,
    pl.Decimal,
}
_INTEGER_DTYPES = {
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
}


def is_numeric_dtype(dtype: pl.DataType) -> bool:
    return dtype in _NUMERIC_DTYPES


def is_integer_dtype(dtype: pl.DataType) -> bool:
    return dtype in _INTEGER_DTYPES


def classify_column_type(series: pl.Series) -> ColumnType:
    dtype = series.dtype
    if dtype == pl.Boolean:
        return ColumnType.BOOLEAN
    if dtype in (pl.Date, pl.Datetime, pl.Time):
        return ColumnType.DATETIME
    if dtype == pl.List:
        return ColumnType.LIST
    if dtype == pl.Struct:
        return ColumnType.STRUCT
    if is_numeric_dtype(dtype):
        return ColumnType.NUMERIC
    if dtype in (pl.Categorical, pl.Enum):
        return ColumnType.CATEGORICAL
    if dtype == pl.Utf8:
        unique_count = series.n_unique()
        return (
            ColumnType.TEXT
            if unique_count > TEXT_CARDINALITY_THRESHOLD
            else ColumnType.CATEGORICAL
        )
    return ColumnType.CATEGORICAL


def infer_column_type(series: pl.Series) -> ColumnType:
    return classify_column_type(series)
