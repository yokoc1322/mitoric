"""Numeric formatting helpers."""

from __future__ import annotations

import math


def _format_float(value: float) -> str:
    if not math.isfinite(value):
        return str(value)
    abs_value = abs(value)
    integer_digits = len(str(int(abs_value))) if abs_value >= 1 else 1
    decimals = max(0, 5 - integer_digits)
    formatted = f"{abs_value:.{decimals}f}"
    return f"-{formatted}" if value < 0 else formatted


def _format_number(value: object, is_integer: bool) -> str:
    if isinstance(value, (int, float)):
        if is_integer and float(value).is_integer():
            return str(int(value))
        if isinstance(value, float):
            return _format_float(value)
        return str(value)
    return str(value)


def _format_bytes(value: object) -> str:
    if not isinstance(value, (int, float)):
        return str(value)
    abs_value = abs(float(value))
    if abs_value < 1024:
        return f"{int(value)} B"

    units = (
        ("G", 1024**3),
        ("M", 1024**2),
        ("K", 1024),
    )
    for suffix, size in units:
        if abs_value >= size:
            scaled = value / size
            if float(scaled).is_integer():
                return f"{int(scaled)}{suffix}"
            return f"{scaled:.1f}{suffix}"
    return f"{int(value)} B"


def _format_number_label(value: float, is_integer: bool) -> str:
    if is_integer and float(value).is_integer():
        return str(int(value))
    return _format_float(float(value))


def _format_numeric_bin_label(lower: float, upper: float, is_integer: bool) -> str:
    lower_label = _format_number_label(lower, is_integer)
    upper_label = _format_number_label(upper, is_integer)
    return (
        lower_label if lower_label == upper_label else f"{lower_label} - {upper_label}"
    )
