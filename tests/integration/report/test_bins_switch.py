from __future__ import annotations

import polars as pl

from mitoric import generate_single_report


def test_bins_switch_has_all_bins() -> None:
    frame = pl.DataFrame({"value": list(range(50))})

    html = generate_single_report(frame)

    assert 'data-bin="10"' in html
    assert 'data-bin="15"' in html
    assert 'data-bin="30"' in html
    assert 'data-bin="50"' in html
