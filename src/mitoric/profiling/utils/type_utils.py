"""Column dtype helpers."""

from __future__ import annotations

import polars as pl

from mitoric.models.base import ColumnType

TEXT_CARDINALITY_THRESHOLD = 100
_NUMERIC_DTYPES: tuple[object, ...] = (
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
    pl.Duration,
)
_INTEGER_DTYPES: tuple[object, ...] = (
    pl.Int8,
    pl.Int16,
    pl.Int32,
    pl.Int64,
    pl.UInt8,
    pl.UInt16,
    pl.UInt32,
    pl.UInt64,
)
_CATEGORICAL_DTYPES: tuple[object, ...] = (pl.Categorical, pl.Enum)
_TEMPORAL_DTYPES: tuple[object, ...] = (pl.Date, pl.Datetime, pl.Time)
_LIST_DTYPES: tuple[object, ...] = (pl.List, pl.Array)
_STRING_DTYPES: tuple[object, ...] = (pl.Utf8, pl.String)
_BASIC_ONLY_DTYPES: tuple[object, ...] = (
    pl.Object,
    pl.Unknown,
    pl.Null,
)


def _matches_dtype(dtype: pl.DataType, targets: tuple[object, ...]) -> bool:
    return any(dtype == target for target in targets)


def _binary_length_or_none(value: object) -> int | None:
    if value is None:
        return None
    if isinstance(value, (bytes, bytearray)):
        return len(value)
    raise TypeError("binary value must be bytes-like")


def is_numeric_dtype(dtype: pl.DataType) -> bool:
    if getattr(dtype, "is_numeric", None) and dtype.is_numeric():
        return True
    if str(dtype).startswith("Duration"):
        return True
    return _matches_dtype(dtype, _NUMERIC_DTYPES)


def is_integer_dtype(dtype: pl.DataType) -> bool:
    if getattr(dtype, "is_integer", None) and dtype.is_integer():
        return True
    return _matches_dtype(dtype, _INTEGER_DTYPES)


def is_temporal_dtype(dtype: pl.DataType) -> bool:
    if dtype == pl.Duration:
        return False
    return (
        _matches_dtype(dtype, _TEMPORAL_DTYPES)
        or getattr(dtype, "is_temporal", lambda: False)()
    )


def is_list_dtype(dtype: pl.DataType) -> bool:
    dtype_name = str(dtype)
    return (
        _matches_dtype(dtype, _LIST_DTYPES)
        or dtype_name.startswith("List")
        or dtype_name.startswith("Array")
    )


def is_categorical_dtype(dtype: pl.DataType) -> bool:
    categories_dtype = getattr(pl, "Categories", None)
    dtype_name = str(dtype)
    if _matches_dtype(dtype, _CATEGORICAL_DTYPES) or dtype_name.startswith(
        "Categorical"
    ):
        return True
    if dtype_name.startswith("Enum"):
        return True
    return bool(categories_dtype and dtype == categories_dtype)


def is_string_dtype(dtype: pl.DataType) -> bool:
    dtype_name = str(dtype)
    return _matches_dtype(dtype, _STRING_DTYPES) or dtype_name.startswith("String")


def is_binary_dtype(dtype: pl.DataType) -> bool:
    return dtype == pl.Binary or str(dtype).startswith("Binary")


def _is_known_dtype(dtype: pl.DataType) -> bool:
    return (
        dtype == pl.Boolean
        or dtype == pl.Struct
        or is_temporal_dtype(dtype)
        or is_list_dtype(dtype)
        or is_numeric_dtype(dtype)
        or is_categorical_dtype(dtype)
        or is_binary_dtype(dtype)
        or is_string_dtype(dtype)
    )


def needs_basic_statistics_only(dtype: pl.DataType) -> bool:
    dtype_name = str(dtype)
    if _matches_dtype(dtype, _BASIC_ONLY_DTYPES) or dtype_name.startswith("Unknown"):
        return True
    return not _is_known_dtype(dtype)


def normalize_numeric_series(series: pl.Series) -> tuple[pl.Series, bool]:
    if is_binary_dtype(series.dtype):
        numeric_series = series.map_elements(
            _binary_length_or_none, return_dtype=pl.UInt32, skip_nulls=False
        )
        return numeric_series.rename(series.name), True

    if series.dtype == pl.Time:
        numeric_series = series.cast(pl.Int64)
        return numeric_series.rename(series.name), True

    if series.dtype == pl.Duration:
        numeric_series = series.cast(pl.Int64).cast(pl.Float64) / 1_000_000_000
        return numeric_series.rename(series.name), False

    return series, is_integer_dtype(series.dtype)


def classify_column_type(series: pl.Series) -> ColumnType:
    dtype = series.dtype
    if dtype == pl.Boolean:
        return ColumnType.BOOLEAN
    if is_temporal_dtype(dtype):
        return ColumnType.DATETIME
    if is_list_dtype(dtype):
        return ColumnType.LIST
    if dtype == pl.Struct:
        return ColumnType.STRUCT
    if is_numeric_dtype(dtype):
        return ColumnType.NUMERIC
    if is_categorical_dtype(dtype):
        return ColumnType.CATEGORICAL
    if is_binary_dtype(dtype):
        return ColumnType.NUMERIC
    if is_string_dtype(dtype):
        unique_count = series.n_unique()
        return (
            ColumnType.TEXT
            if unique_count > TEXT_CARDINALITY_THRESHOLD
            else ColumnType.CATEGORICAL
        )
    return ColumnType.CATEGORICAL


def infer_column_type(series: pl.Series) -> ColumnType:
    return classify_column_type(series)
