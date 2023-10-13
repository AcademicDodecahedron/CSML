from __future__ import annotations
from typing import Any, Iterable, MutableMapping, Optional
from jinja2 import Environment, Template
import textwrap

from .extension import SqltypedExtension
from .convert import default_converter, Sql
from .types import Column


def _sqltyped_filter(value: Any) -> str:
    return default_converter.convert(value)


def _sqljoin_filter(
    values: Iterable[Any],
    delimiter: str,
    attribute: Optional[str] = None,
) -> Sql:
    return Sql(
        delimiter.join(
            map(
                lambda value: default_converter.convert(
                    getattr(value, attribute) if attribute else value
                ),
                values,
            )
        )
    )


def _sql_filter(value: str) -> Sql:
    return Sql(value)


class SqlEnvironment(Environment):
    def __init__(self):
        super().__init__()

        self._prepare_environment()

    def _prepare_environment(self):
        self.add_extension(SqltypedExtension)

        self.globals["Column"] = Column
        self.filters["sqltyped"] = _sqltyped_filter
        self.filters["sqljoin"] = _sqljoin_filter
        self.filters["sql"] = _sql_filter

    def from_string(
        self,
        source: str,
        globals: Optional[MutableMapping[str, Any]] = None,
    ) -> Template:
        return super().from_string(textwrap.dedent(source).strip(), globals)

    def render(self, *args_all, **kwargs) -> str:
        if len(args_all) == 0 or not isinstance(args_all[0], str):
            raise ValueError("First argument must be a string template")

        # Can't have `source` as an argument name,
        # since that would collide with 'source' being use as a keyword argument name
        source, args = args_all[0], args_all[1:]

        return self.from_string(source).render(*args, **kwargs)


default_environment = SqlEnvironment()
