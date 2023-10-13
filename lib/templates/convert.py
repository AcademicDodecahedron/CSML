from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional
from sqlglot import exp
import inspect

from jinja2.runtime import Context as JinjaContext


class ToSql(ABC):
    @abstractmethod
    def sql(self, context: JinjaContext) -> str:
        ...


@dataclass
class Sql(ToSql):
    value: str

    def sql(self) -> str:
        return self.value


class Converter:
    def __init__(self, mapping: Optional[dict] = None) -> None:
        self._mapping = mapping or {
            str: lambda value: exp.Literal.string(value).sql(),
            int: lambda value: exp.Literal.number(value).sql(),
            float: lambda value: exp.Literal.number(value).sql(),
            exp.Expression: lambda value: value.sql(),
            ToSql: lambda value: value.sql(),
        }

    def register_type(self, type_: type, to_sql: Callable[[Any, JinjaContext], str]):
        self._mapping[type_] = to_sql

    def convert(self, value: Any) -> str:
        for parent_class in inspect.getmro(type(value)):
            if parent_class in self._mapping:
                return self._mapping[parent_class](value)

        raise KeyError("Unsupported type", type(value))


default_converter = Converter()
