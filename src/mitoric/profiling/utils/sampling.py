"""Utility functions for sampling representative values."""

from __future__ import annotations

import json

import polars as pl

from mitoric.profiling.utils.constants import SAMPLE_VALUES_LIMIT


def collect_sample_values(
    series: pl.Series, *, limit: int = SAMPLE_VALUES_LIMIT
) -> list[str]:
    """Collect sample values from a series for display in reports.

    Parameters
    ----------
    series:
        Target series to sample. Null values are ignored.
    limit:
        Maximum number of samples to collect.
    """

    samples: list[str] = []
    for value in series.drop_nulls().head(limit).to_list():
        samples.append(_stringify_value(value))
    return samples


def _stringify_value(value: object) -> str:
    if isinstance(value, (dict, list, tuple)):
        try:
            return json.dumps(value, ensure_ascii=False)
        except TypeError:
            return repr(value)
    if isinstance(value, (bytes, bytearray)):
        return value.hex()
    return str(value)
