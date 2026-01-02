from __future__ import annotations

import polars as pl

from mitoric.profiling.associations import compute_associations


def test_associations_with_nulls_and_zero_variance() -> None:
    frame = pl.DataFrame(
        {
            "x": [1.0, 1.0, None],
            "y": [2.0, 2.0, 2.0],
        }
    )

    summary = compute_associations(frame)

    assert summary.numeric_numeric[0].value == 0.0


def test_categorical_associations_single_category() -> None:
    frame = pl.DataFrame(
        {
            "left": ["A", "A", "A"],
            "right": ["B", "B", "B"],
        }
    )

    summary = compute_associations(frame)

    assert summary.categorical_categorical[0].value == 0.0


def test_mixed_associations_single_pair_returns_zero() -> None:
    frame = pl.DataFrame(
        {
            "numeric": [1.0, None, 3.0],
            "category": ["A", "B", None],
        }
    )

    summary = compute_associations(frame)

    assert summary.numeric_categorical[0].value == 0.0
