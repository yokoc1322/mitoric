from __future__ import annotations

import datetime as dt

import polars as pl
import pytest

from mitoric.models.base import ColumnName, ColumnType
from mitoric.profiling.columns import profile_columns


def test_numeric_histogram_constant_values() -> None:
    frame = pl.DataFrame({"value": [2, 2, 2]})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    numeric = profile_map[ColumnName("value")].numeric_profile

    assert numeric is not None
    for histogram in numeric.histograms:
        assert len(histogram.bins) == 1
        assert histogram.bins[0].lower == 2
        assert histogram.bins[0].upper == 2
        assert histogram.bins[0].count == 3


def test_numeric_histogram_low_cardinality_bins() -> None:
    frame = pl.DataFrame({"value": [1, 2, 2, 3, 3, 3]})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    numeric = profile_map[ColumnName("value")].numeric_profile

    assert numeric is not None
    assert len(numeric.histograms) == 1
    histogram = numeric.histograms[0]
    assert histogram.bin_count == 3
    assert [(bin.lower, bin.upper, bin.count) for bin in histogram.bins] == [
        (1, 1, 1),
        (2, 2, 2),
        (3, 3, 3),
    ]


def test_numeric_outlier_rate_zero_iqr() -> None:
    frame = pl.DataFrame({"value": [5, 5, 5]})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    numeric = profile_map[ColumnName("value")].numeric_profile

    assert numeric is not None
    assert numeric.outlier_rate == 0.0


def test_categorical_high_cardinality_suppressed_count() -> None:
    values = [f"cat_{idx}" for idx in range(120)]
    series = pl.Series("category", values, dtype=pl.Categorical)
    frame = pl.DataFrame({"category": series})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    categorical = profile_map[ColumnName("category")].categorical_profile

    assert categorical is not None
    assert categorical.is_high_cardinality is True
    assert len(categorical.top_categories) == 10
    assert categorical.suppressed_count == 110
    assert categorical.histograms


def test_datetime_profile_empty_values() -> None:
    series = pl.Series("when", [None, None], dtype=pl.Date)
    frame = pl.DataFrame({"when": series})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    datetime_profile = profile_map[ColumnName("when")].datetime_profile

    assert datetime_profile is not None
    assert datetime_profile.min_datetime == ""
    assert datetime_profile.max_datetime == ""
    assert datetime_profile.histograms == []
    assert datetime_profile.top_values == []


def test_boolean_histogram() -> None:
    frame = pl.DataFrame({"flag": [True, False, True, None]})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    boolean_profile = profile_map[ColumnName("flag")]

    assert boolean_profile.data_type == ColumnType.BOOLEAN
    assert boolean_profile.categorical_profile is not None
    assert boolean_profile.categorical_profile.histograms
    histogram = boolean_profile.categorical_profile.histograms[0]
    assert histogram.labels == ["True", "False"]
    assert histogram.counts == [2, 1]


def test_datetime_histogram_and_top_values() -> None:
    frame = pl.DataFrame(
        {
            "birthdate": [
                dt.date(2024, 1, 1),
                dt.date(2024, 1, 2),
                dt.date(2024, 1, 2),
                dt.date(2024, 1, 3),
            ]
        }
    )

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    datetime_profile = profile_map[ColumnName("birthdate")].datetime_profile

    assert datetime_profile is not None
    assert datetime_profile.histograms
    histogram = datetime_profile.histograms[0]
    assert histogram.labels == ["2024-01-01", "2024-01-02", "2024-01-03"]
    assert histogram.counts == [1, 2, 1]
    assert datetime_profile.top_values[0].value == "2024-01-02"
    assert datetime_profile.top_values[0].count == 2


def test_categorical_histogram_low_cardinality() -> None:
    frame = pl.DataFrame({"category": ["A", "B", "A", "C"]})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    categorical = profile_map[ColumnName("category")].categorical_profile

    assert categorical is not None
    assert categorical.histograms
    histogram = categorical.histograms[0]
    assert histogram.labels == ["A", "B", "C"]
    assert histogram.counts == [2, 1, 1]


def test_text_length_histogram() -> None:
    values = [f"{idx:03d}" for idx in range(101)]
    frame = pl.DataFrame({"text": values})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    text_profile = profile_map[ColumnName("text")].text_profile

    assert text_profile is not None
    assert text_profile.length_histograms
    histogram = text_profile.length_histograms[0]
    assert histogram.bin_count == 1
    assert [(bin.lower, bin.upper, bin.count) for bin in histogram.bins] == [
        (3, 3, 101),
    ]


def test_numeric_histogram_integer_edges() -> None:
    values = list(range(21))
    frame = pl.DataFrame({"value": values})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    numeric = profile_map[ColumnName("value")].numeric_profile

    assert numeric is not None
    assert numeric.is_integer is True
    histogram = next(
        histogram for histogram in numeric.histograms if histogram.bin_count == 10
    )
    for bin in histogram.bins:
        assert float(bin.lower).is_integer()
        assert float(bin.upper).is_integer()


def test_list_profile_with_length_stats() -> None:
    frame = pl.DataFrame({"items": [[1, 2], [3], None]})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    list_profile = profile_map[ColumnName("items")]

    assert list_profile.data_type == ColumnType.LIST
    assert list_profile.non_null_count == 2
    assert list_profile.null_count == 1
    assert list_profile.unique_count == 3
    assert list_profile.list_profile is not None
    assert list_profile.list_profile.length_stats.minimum == 1
    assert list_profile.list_profile.length_stats.maximum == 2
    assert list_profile.list_profile.length_stats.mean == pytest.approx(1.5)
    assert list_profile.list_profile.length_stats.median == pytest.approx(1.5)


def test_struct_profile_limits_analysis() -> None:
    frame = pl.DataFrame(
        {
            "info": [
                {"a": 1, "b": "x"},
                {"a": 2, "b": "y"},
                None,
            ]
        }
    )

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    struct_profile = profile_map[ColumnName("info")]

    assert struct_profile.data_type == ColumnType.STRUCT
    assert struct_profile.non_null_count == 2
    assert struct_profile.null_count == 1
    assert struct_profile.unique_count == 3
    assert struct_profile.numeric_profile is None
    assert struct_profile.categorical_profile is None
    assert struct_profile.text_profile is None
    assert struct_profile.datetime_profile is None


def test_binary_column_profile_uses_lengths() -> None:
    frame = pl.DataFrame(
        {"payload": [b"", b"abc", None]}, schema={"payload": pl.Binary}
    )

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    binary_profile = profile_map[ColumnName("payload")]

    assert binary_profile.data_type == ColumnType.NUMERIC
    assert binary_profile.zero_count == 1
    assert binary_profile.numeric_profile is not None
    assert binary_profile.numeric_profile.stats.minimum == 0.0
    assert binary_profile.numeric_profile.stats.maximum == 3.0


def test_object_column_skips_detailed_profiles() -> None:
    frame = pl.DataFrame({"object_col": [object(), None]})

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    object_profile = profile_map[ColumnName("object_col")]

    assert object_profile.data_type == ColumnType.CATEGORICAL
    assert object_profile.categorical_profile is None
    assert object_profile.numeric_profile is None
    assert object_profile.text_profile is None
    assert object_profile.datetime_profile is None


def test_array_column_uses_list_profile() -> None:
    frame = pl.DataFrame(
        {"array_col": pl.Series([[1, 2], [3, 4], None], dtype=pl.Array(pl.Int64, 2))}
    )

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    array_profile = profile_map[ColumnName("array_col")]

    assert array_profile.data_type == ColumnType.LIST
    assert array_profile.list_profile is not None
    assert array_profile.list_profile.length_stats.minimum == 2


def test_struct_with_null_field_unique_count() -> None:
    """Test that Struct columns with Null-typed fields don't cause PanicException in n_unique."""
    frame = pl.DataFrame(
        {
            "data": [
                {"id": "a", "container": None},
                {"id": "b", "container": None},
                {"id": "a", "container": None},
                None,
            ]
        },
        schema={
            "data": pl.Struct({"id": pl.String, "container": pl.Null}),
        },
    )

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    struct_profile = profile_map[ColumnName("data")]

    assert struct_profile.data_type == ColumnType.STRUCT
    assert struct_profile.unique_count == 3  # 2 unique non-null + 1 null
    assert struct_profile.null_count == 1
    assert struct_profile.non_null_count == 3


def test_list_of_struct_unique_count() -> None:
    """Test that List[Struct] columns don't cause InvalidOperationError in n_unique."""
    frame = pl.DataFrame(
        {
            "items": [
                [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                [{"id": 1, "name": "a"}, {"id": 2, "name": "b"}],
                [{"id": 3, "name": "c"}],
                None,
            ]
        }
    )

    profiles = profile_columns(frame, target_columns=None, explicit_types=None)
    profile_map = {profile.column_name: profile for profile in profiles}
    list_profile = profile_map[ColumnName("items")]

    assert list_profile.data_type == ColumnType.LIST
    assert list_profile.unique_count == 3  # 2 unique non-null + 1 null
    assert list_profile.null_count == 1
    assert list_profile.non_null_count == 3
