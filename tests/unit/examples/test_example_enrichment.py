from __future__ import annotations

import datetime as dt

import polars as pl

from tests.e2e.example_reports import _enrich_example_frame, _split_frame_by_survived


def test_enrich_example_frame_adds_birthdate_and_boolean() -> None:
    frame = pl.DataFrame(
        {
            "age": [0.0, 1.0, 2.5, None],
            "survived": [1, 0, None, 1],
        }
    )

    enriched = _enrich_example_frame(frame)

    reference_date = dt.datetime(1912, 4, 14)
    expected_birthdates = [
        reference_date - dt.timedelta(days=0.0),
        reference_date - dt.timedelta(days=365.25),
        reference_date - dt.timedelta(days=365.25 * 2.5),
        None,
    ]

    assert enriched["survived"].to_list() == [True, False, None, True]
    assert enriched["birthdate"].to_list() == expected_birthdates


def test_split_frame_by_survived_drops_nulls_and_partitions() -> None:
    frame = pl.DataFrame(
        {
            "age": [0.0, 1.0, 2.5, None],
            "survived": [1, 0, None, 1],
            "fare": [10.0, 20.0, 30.0, 40.0],
        }
    )

    enriched = _enrich_example_frame(frame)
    survived_frame, not_survived_frame = _split_frame_by_survived(enriched)

    assert survived_frame["survived"].unique().to_list() == [True]
    assert not_survived_frame["survived"].unique().to_list() == [False]
    assert survived_frame.height == 2
    assert not_survived_frame.height == 1
