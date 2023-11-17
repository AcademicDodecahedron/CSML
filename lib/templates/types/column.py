from __future__ import annotations
from typing import Generic, TypeVar
from jinja2 import Template
from sqlglot import exp

from .primitives import identifier
from ..adapter import Sql, ToSql, sql_adapter

TYPE = TypeVar("TYPE")


class ColumnBase(Generic[TYPE]):
    def __init__(self, name: str, type: TYPE) -> None:
        self.name = name
        self.type = type


class Column(ColumnBase[Template]):
    def __init__(self, name: str, type: str) -> None:
        from lib.templates.environment import sql_environment

        super().__init__(name, sql_environment.from_string(type))

    def render(self, **params) -> ColumnRendered:
        return ColumnRendered(self.name, Sql(self.type.render(**params)))


class ColumnRendered(ColumnBase[Sql]):
    def __init__(self, name: str, type: Sql) -> None:
        super().__init__(name, type)

    def identifier(self) -> exp.Identifier:
        return identifier(self.name)

    def definition(self) -> ColumnDefinition:
        return ColumnDefinition(self)

    def render(self, **params) -> ColumnRendered:
        return self


class ColumnDefinition(ToSql):
    def __init__(self, column: ColumnRendered) -> None:
        self.column = column

    def sql(self) -> str:
        return sql_adapter.format("{} {}", self.column.identifier(), self.column.type)


__all__ = ["Column", "ColumnRendered", "ColumnDefinition"]
