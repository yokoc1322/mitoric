from __future__ import annotations

import polars as pl
import pytest

from mitoric.profiling.associations import compute_associations


def test_association_metrics() -> None:
    frame = pl.DataFrame(
        {
            "x": [1, 2, 3, 4],
            "y": [2, 4, 6, 8],
            "cat_left": ["A", "A", "B", "B"],
            "cat_right": ["X", "X", "Y", "Y"],
            "score": [1, 1, 3, 3],
        }
    )

    summary = compute_associations(frame)

    numeric_pair = summary.numeric_numeric[0]
    assert numeric_pair.value == pytest.approx(1.0)

    categorical_pair = summary.categorical_categorical[0]
    assert categorical_pair.value == pytest.approx(1.0)

    mixed_pair = summary.numeric_categorical[0]
    assert mixed_pair.value == pytest.approx(1.0)


def test_association_top_20_limit() -> None:
    frame = pl.DataFrame({f"n{i}": [1, 2, 3, 4] for i in range(7)})

    summary = compute_associations(frame)

    assert len(summary.numeric_numeric) == 20


def test_associations_sorted_by_value() -> None:
    frame = pl.DataFrame(
        {
            "x": [1, 2, 3, 4, 5],
            "y": [1, 2, 3, 5, 7],
            "z": [7, 5, 4, 3, 1],
        }
    )

    summary = compute_associations(frame)

    values = [item.value for item in summary.numeric_numeric]
    assert values == sorted(values, reverse=True)
