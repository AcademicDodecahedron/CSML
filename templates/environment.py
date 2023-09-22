from __future__ import annotations
from typing import Any, Iterable, MutableMapping, Optional
from jinja2 import Environment, Template
import textwrap

from .extension import SqltypedExtension
from .convert import Converter, Sql


def sqltyped_filter(value: Any) -> str:
    return Converter.default.convert(value)


def sqljoin_filter(
    values: Iterable[Any],
    delimiter: str,
    attribute: Optional[str] = None,
) -> Sql:
    return Sql(
        delimiter.join(
            map(
                lambda value: Converter.default.convert(
                    getattr(value, attribute) if attribute else value
                ),
                values,
            )
        )
    )


def sql_filter(value: str) -> Sql:
    return Sql(value)


class SqlEnvironment(Environment):
    default: SqlEnvironment

    def __init__(self):
        super().__init__()

        self._prepare_environment()

    def _prepare_environment(self):
        self.add_extension(SqltypedExtension)
        self.filters["sqltyped"] = sqltyped_filter
        self.filters["sqljoin"] = sqljoin_filter
        self.filters["sql"] = sql_filter

    def from_string(
        self,
        source: str,
        globals: Optional[MutableMapping[str, Any]] = None,
    ) -> Template:
        return super().from_string(textwrap.dedent(source).strip(), globals)


SqlEnvironment.default = SqlEnvironment()
