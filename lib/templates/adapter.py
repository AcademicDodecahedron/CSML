from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Callable, Optional
from sqlglot import exp
import inspect

from jinja2.runtime import Context as JinjaContext


class ToSql(ABC):
    @abstractmethod
    def sql(self) -> str:
        ...


@dataclass
class Sql(ToSql):
    value: str

    def sql(self) -> str:
        return self.value


class SqlAdapter:
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

    def adapt(self, value: Any) -> str:
        for parent_class in inspect.getmro(type(value)):
            if parent_class in self._mapping:
                return self._mapping[parent_class](value)

        raise KeyError("Unsupported type", type(value))

    def format(self, *args_all, **kwargs) -> str:
        if len(args_all) == 0 or not isinstance(args_all[0], str):
            raise ValueError("First argument must be a string template")

        # Can't have `source` as an argument name,
        # since that would collide with 'source' being use as a keyword argument name
        source, args = args_all[0], args_all[1:]

        return source.format(
            *map(self.adapt, args),
            **{key: self.adapt(value) for key, value in kwargs.items()},
        )


sql_adapter = SqlAdapter()
