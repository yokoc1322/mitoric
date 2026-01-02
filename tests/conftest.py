from __future__ import annotations

import sys
from pathlib import Path

import polars as pl
import pytest

PROJECT_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PROJECT_ROOT / "src"
for path in (SRC_ROOT, PROJECT_ROOT):
    if str(path) not in sys.path:
        sys.path.append(str(path))


@pytest.fixture
def sample_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "age": [10, 12, 12, 14],
            "city": ["A", "B", "A", "C"],
        }
    )
