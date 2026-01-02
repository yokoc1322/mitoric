from __future__ import annotations

from mitoric.render.formatters import (
    _format_bytes,
    _format_float,
    _format_number_label,
    _format_numeric_bin_label,
)


def test_format_float_six_chars() -> None:
    assert _format_float(12.3456) == "12.346"
    assert _format_float(0.1) == "0.1000"
    assert _format_float(1234.56) == "1234.6"


def test_format_bytes_with_units() -> None:
    assert _format_bytes(512) == "512 B"
    assert _format_bytes(1536) == "1.5K"
    assert _format_bytes(1024 * 1024) == "1M"
    assert _format_bytes(1024 * 1024 * 1024) == "1G"


def test_format_number_label_for_integer_values() -> None:
    assert _format_number_label(10.0, is_integer=True) == "10"
    assert _format_number_label(10.5, is_integer=True) == "10.500"


def test_format_numeric_bin_label_collapses_equal_bounds() -> None:
    assert _format_numeric_bin_label(1.0, 1.0, is_integer=True) == "1"
    assert _format_numeric_bin_label(1.0, 2.0, is_integer=True) == "1 - 2"
