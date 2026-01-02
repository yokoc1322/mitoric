from __future__ import annotations

import polars as pl

from mitoric import generate_single_report


def test_report_interactions_metadata() -> None:
    ages = list(range(21))
    frame = pl.DataFrame(
        {"age": ages, "city": ["A" if value % 2 == 0 else "B" for value in ages]}
    )

    html = generate_single_report(frame)

    assert 'data-column-name="age"' in html
    assert 'data-column-name="city"' in html
    assert 'data-section="variables"' in html
    assert 'data-section="associations"' in html
    assert 'data-bin="10"' in html
    assert 'data-bin="15"' in html
    assert 'data-bin="30"' in html
    assert 'data-bin="50"' in html
