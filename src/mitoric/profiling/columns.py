"""Column-level profiling."""

from __future__ import annotations

import polars as pl

from mitoric.models.aggregation import ColumnProfile, CompareColumnProfile
from mitoric.models.base import (
    ColumnName,
    ColumnType,
    ExplicitType,
    NonNullCount,
    NullCount,
    NullRate,
    UniqueCount,
    ZeroCount,
)
from mitoric.profiling.compare.histograms import build_compare_histograms_for_column
from mitoric.profiling.profiles.categorical import build_categorical_profile
from mitoric.profiling.profiles.datetime import build_datetime_profile
from mitoric.profiling.profiles.list_profile import build_list_profile
from mitoric.profiling.profiles.numeric import build_numeric_profile
from mitoric.profiling.profiles.text import build_text_profile
from mitoric.profiling.utils.type_utils import (
    infer_column_type,
    is_integer_dtype,
    is_numeric_dtype,
)


def _apply_explicit_type(
    column_name: ColumnName, explicit_types: list[ExplicitType], inferred: ColumnType
) -> ColumnType:
    for explicit in explicit_types:
        if explicit.column_name == column_name:
            return explicit.data_type
    return inferred


def profile_columns(
    frame: pl.DataFrame,
    *,
    target_columns: list[str] | None,
    explicit_types: list[ExplicitType] | None,
) -> list[ColumnProfile]:
    explicit_list = explicit_types or []
    target_set = {ColumnName(name) for name in target_columns or []}
    profiles: list[ColumnProfile] = []
    row_count = frame.height

    for name in frame.columns:
        series = frame.get_column(name)
        column_name = ColumnName(name)
        inferred = infer_column_type(series)
        data_type = _apply_explicit_type(column_name, explicit_list, inferred)

        null_count = series.null_count()
        non_null_count = row_count - null_count
        null_rate = null_count / row_count if row_count else 0.0
        unique_count = series.n_unique()
        zero_count = 0
        if is_numeric_dtype(series.dtype):
            zero_count = int(series.drop_nulls().eq(0).sum())

        include_details = not target_set or column_name in target_set

        numeric_profile = None
        categorical_profile = None
        text_profile = None
        datetime_profile = None
        list_profile = None

        if include_details:
            if data_type == ColumnType.NUMERIC:
                is_integer = is_integer_dtype(series.dtype)
                numeric_profile = build_numeric_profile(
                    series.drop_nulls().cast(pl.Float64), is_integer=is_integer
                )
            elif data_type in (ColumnType.CATEGORICAL, ColumnType.BOOLEAN):
                category_series = series.drop_nulls().cast(pl.Utf8)
                if data_type == ColumnType.BOOLEAN:
                    category_series = category_series.str.to_titlecase()
                categorical_profile = build_categorical_profile(
                    category_series, unique_count
                )
            elif data_type == ColumnType.TEXT:
                text_profile = build_text_profile(series.drop_nulls().cast(pl.Utf8))
            elif data_type == ColumnType.DATETIME:
                datetime_profile = build_datetime_profile(series.drop_nulls())
            elif data_type == ColumnType.LIST:
                list_profile = build_list_profile(series)

        profiles.append(
            ColumnProfile(
                column_name=column_name,
                data_type=data_type,
                non_null_count=NonNullCount(non_null_count),
                null_count=NullCount(null_count),
                null_rate=NullRate(null_rate),
                unique_count=UniqueCount(unique_count),
                zero_count=ZeroCount(zero_count),
                numeric_profile=numeric_profile,
                categorical_profile=categorical_profile,
                text_profile=text_profile,
                datetime_profile=datetime_profile,
                list_profile=list_profile,
            )
        )

    return profiles


def compare_column_profiles(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    target_columns: list[str] | None = None,
    explicit_types: list[ExplicitType] | None = None,
) -> tuple[list[ColumnProfile], list[ColumnProfile]]:
    target_set = set(target_columns) if target_columns else None
    left_column_names = [
        name for name in left.columns if target_set is None or name in target_set
    ]
    right_column_names = [
        name for name in right.columns if target_set is None or name in target_set
    ]
    left_only_columns = [
        name for name in left_column_names if name not in right_column_names
    ]
    right_only_columns = [
        name for name in right_column_names if name not in left_column_names
    ]

    left_profiles = (
        profile_columns(
            left.select(left_only_columns),
            target_columns=None,
            explicit_types=explicit_types,
        )
        if left_only_columns
        else []
    )
    right_profiles = (
        profile_columns(
            right.select(right_only_columns),
            target_columns=None,
            explicit_types=explicit_types,
        )
        if right_only_columns
        else []
    )
    return left_profiles, right_profiles


def compare_common_column_profiles(
    left: pl.DataFrame,
    right: pl.DataFrame,
    *,
    target_columns: list[str] | None = None,
    explicit_types: list[ExplicitType] | None = None,
) -> list[CompareColumnProfile]:
    target_set = set(target_columns) if target_columns else None
    left_column_names = [
        name for name in left.columns if target_set is None or name in target_set
    ]
    right_column_names = [
        name for name in right.columns if target_set is None or name in target_set
    ]
    common_columns = [name for name in left_column_names if name in right_column_names]
    if not common_columns:
        return []

    left_profiles = profile_columns(
        left.select(common_columns),
        target_columns=None,
        explicit_types=explicit_types,
    )
    right_profiles = profile_columns(
        right.select(common_columns),
        target_columns=None,
        explicit_types=explicit_types,
    )
    left_by_name = {profile.column_name: profile for profile in left_profiles}
    right_by_name = {profile.column_name: profile for profile in right_profiles}

    compare_profiles: list[CompareColumnProfile] = []
    for column_name in common_columns:
        left_profile = left_by_name.get(ColumnName(column_name))
        right_profile = right_by_name.get(ColumnName(column_name))
        if left_profile is None or right_profile is None:
            continue
        histograms = build_compare_histograms_for_column(
            left[column_name],
            right[column_name],
            left_profile.data_type,
            right_profile.data_type,
        )
        compare_profiles.append(
            CompareColumnProfile(
                column_name=ColumnName(column_name),
                left_profile=left_profile,
                right_profile=right_profile,
                histograms=histograms,
            )
        )
    return compare_profiles
