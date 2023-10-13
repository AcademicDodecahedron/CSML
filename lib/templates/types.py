from __future__ import annotations
from typing import Any, Optional
from sqlglot import exp
from dataclasses import dataclass
from returns.maybe import Maybe

from .adapter import ToSql, sql_adapter


def identifier(name: str) -> exp.Identifier:
    return exp.Identifier(this=name)


def table(name: str, schema: Optional[str] = None) -> exp.Table:
    return exp.Table(
        this=identifier(name),
        db=Maybe.from_optional(schema).map(identifier).value_or(None),
    )


def placeholder(name: Optional[str] = None) -> exp.Placeholder:
    return exp.Placeholder(this=name)


class Column:
    def __init__(self, name: str, type: str) -> None:
        self.name = name
        self.type = type

    def render(self, **params):
        from .environment import sql_environment

        return ColumnRendered(self.name, sql_environment.render(self.type, **params))


@dataclass
class ColumnRendered:
    name: str
    type: str

    def identifier(self) -> exp.Identifier:
        return identifier(self.name)

    def definition(self) -> ColumnDefinition:
        return ColumnDefinition(self)


_FROM_NAME = object()


class ValueColumn:
    def __init__(self, name: str, type: str, value: Any = _FROM_NAME) -> None:
        self.name = name
        self.type = type
        self.value = placeholder(name) if value == _FROM_NAME else value

    def render(self, **params):
        from .environment import sql_environment

        return ValueColumnRendered(
            self.name,
            sql_environment.from_string(self.type).render(**params),
            self.value,
        )


@dataclass
class ValueColumnRendered(ColumnRendered):
    value: Any

    def set_statement(self) -> ValueColumnSetStatement:
        return ValueColumnSetStatement(self)


class ColumnDefinition(ToSql):
    def __init__(self, column: ColumnRendered) -> None:
        self.column = column

    def sql(self) -> str:
        return f"{self.column.identifier()} {self.column.type}"


class ValueColumnSetStatement(ToSql):
    def __init__(self, value_column: ValueColumnRendered) -> None:
        self._value_column = value_column

    def sql(self) -> str:
        return f"{self._value_column.identifier} = {sql_adapter.adapt(self._value_column.value)}"
