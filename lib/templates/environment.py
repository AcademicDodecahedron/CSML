from __future__ import annotations
from typing import Any, Callable, Iterable, MutableMapping, Optional
from jinja2 import Environment, Template
import jinja2.utils
import textwrap

from .extension import SqltypedExtension
from .adapter import sql_adapter, Sql
from .types import Column


def _sqltyped_filter(value: Any) -> str:
    return sql_adapter.adapt(value)


def _sqljoin_filter(
    values: Iterable[Any],
    delimiter: str,
    attribute: Optional[str] = None,
) -> Sql:
    return Sql(
        delimiter.join(
            map(
                lambda value: sql_adapter.adapt(
                    getattr(value, attribute) if attribute else value
                ),
                values,
            )
        )
    )


def _sql_filter(value: str) -> Sql:
    return Sql(value)

def _sqljoiner(sep: str = ", ") -> Callable[[], Sql]:
    joiner = jinja2.utils.Joiner(sep)
    return lambda: Sql(joiner())


class SqlEnvironment(Environment):
    def __init__(self):
        super().__init__()

        self._prepare_environment()

    def _prepare_environment(self):
        self.add_extension(SqltypedExtension)

        self.globals["Column"] = Column
        self.globals["sqljoiner"] = _sqljoiner

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


sql_environment = SqlEnvironment()
