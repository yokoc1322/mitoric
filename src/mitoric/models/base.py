"""Shared domain types across profiling and report layers."""

from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
from typing import NewType

DatasetId = NewType("DatasetId", str)
ReportId = NewType("ReportId", str)
ColumnName = NewType("ColumnName", str)


class ColumnType(str, Enum):
    NUMERIC = "numeric"
    CATEGORICAL = "categorical"
    TEXT = "text"
    DATETIME = "datetime"
    BOOLEAN = "boolean"
    LIST = "list"
    STRUCT = "struct"

    @classmethod
    def from_raw(cls, value: ColumnType | str) -> ColumnType:
        if isinstance(value, cls):
            return value
        return cls(str(value))

    @classmethod
    def allowed(cls) -> set[ColumnType]:
        return set(cls)

    @property
    def label(self) -> str:
        return self.value

    def __str__(self) -> str:
        return self.value


ReportMode = NewType("ReportMode", str)
SavePath = NewType("SavePath", str)

RowCount = NewType("RowCount", int)
ColumnCount = NewType("ColumnCount", int)
MemoryBytes = NewType("MemoryBytes", int)
MissingCount = NewType("MissingCount", int)
MissingRate = NewType("MissingRate", float)
DuplicateRowCount = NewType("DuplicateRowCount", int)
UniqueCount = NewType("UniqueCount", int)
ZeroCount = NewType("ZeroCount", int)
NonNullCount = NewType("NonNullCount", int)
NullCount = NewType("NullCount", int)
NullRate = NewType("NullRate", float)
OutlierRate = NewType("OutlierRate", float)
SuppressedCount = NewType("SuppressedCount", int)
AssociationValue = NewType("AssociationValue", float)
RowCountDelta = NewType("RowCountDelta", int)
WarningMessage = NewType("WarningMessage", str)
HtmlString = NewType("HtmlString", str)
GeneratedAt = NewType("GeneratedAt", str)


@dataclass(frozen=True)
class ExplicitType:
    column_name: ColumnName
    data_type: ColumnType
