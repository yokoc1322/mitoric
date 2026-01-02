from __future__ import annotations

import polars as pl

from mitoric.api.pipeline import CompareReportRequest, SingleReportRequest
from mitoric.models.base import ColumnName, DatasetId, SavePath


def test_single_report_request_normalizes_inputs() -> None:
    frame = pl.DataFrame({"value": [1, 2, 3]})

    request = SingleReportRequest.from_raw(
        frame,
        target_columns=["value"],
        explicit_types=None,
        save_path=None,
    )

    assert request.target_columns == [ColumnName("value")]
    assert request.explicit_types == []
    assert request.save_path == SavePath("")


def test_compare_report_request_normalizes_labels() -> None:
    left = pl.DataFrame({"value": [1, 2, 3]})
    right = pl.DataFrame({"value": [4, 5, 6]})

    request = CompareReportRequest.from_raw(
        left,
        right,
        target_columns=None,
        explicit_types=None,
        save_path=None,
        left_name="  ",
        right_name=None,
    )

    assert request.left_name == DatasetId("left")
    assert request.right_name == DatasetId("right")
