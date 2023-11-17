from __future__ import annotations
from typing import Any

from .primitives import identifier, placeholder
from .column import Column, ColumnRendered
from ..adapter import ToSql, sql_adapter


FROM_NAME = object()


def infer_value(name: str, value: Any = FROM_NAME) -> Any:
    return placeholder(name) if value == FROM_NAME else value


class ValueColumn(Column):
    def __init__(self, name: str, type: str, value: Any = FROM_NAME) -> None:
        super().__init__(name, type)
        self.value = infer_value(name, value)

    def render(self, **params) -> ValueColumnRendered:
        return ValueColumnRendered(self.name, self.type.render(**params), self.value)


class ValueColumnRendered(ColumnRendered):
    def __init__(self, name: str, type: str, value: Any = FROM_NAME) -> None:
        super().__init__(name, type)
        self.value = infer_value(name, value)

    def set_statement(self) -> ValueColumnSetStatement:
        return ValueColumnSetStatement(self)

    def render(self, **params) -> ValueColumnRendered:
        return self


class ValueColumnSetStatement(ToSql):
    def __init__(self, value_column: ValueColumnRendered) -> None:
        self.value_column = value_column

    def sql(self) -> str:
        return sql_adapter.format(
            "{} = {}", self.value_column.identifier(), self.value_column.value
        )


class IdColumn(ToSql):
    def __init__(self, name: str, value: Any = FROM_NAME) -> None:
        self.name = name
        self.value = infer_value(name, value)

    def sql(self) -> str:
        return sql_adapter.format("{} = {}", identifier(self.name), self.value)


__all__ = ["ValueColumn", "ValueColumnRendered", "ValueColumnSetStatement", "IdColumn"]
