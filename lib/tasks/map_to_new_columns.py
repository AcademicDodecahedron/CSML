from sqlite3 import Connection
from typing_extensions import Callable
from sqlglot.expressions import Table

from lib.checks import columns_exist
from lib.console import console
from ..templates import ValueColumn, IdColumn, sql_environment
from .base import Task
from .row_factory import with_dict_factory


class Templates:
    add_column = sql_environment.from_string(
        """\
        ALTER TABLE {{table}}
        ADD COLUMN {{ column.definition() }};
        """
    )
    update_table = sql_environment.from_string(
        """\
        {% set sep = joiner(',\\n') -%}
        UPDATE {{table}} SET
        {% for column in columns -%}
        {{ sep() | sql }}{{ column.identifier() }} = {{ column.value }}
        {%- endfor %}
        WHERE
        {{ id_fields | sqljoin(' AND ') }};
        """
    )


class MapToNewColumns(Task):
    def __init__(
        self,
        select: str,
        table: Table,
        columns: list[ValueColumn],
        fn: Callable[..., dict],
        id_fields: list[IdColumn],
        params: dict = {},
    ) -> None:
        super().__init__()

        self._fn = fn
        self._table = table
        params_merged = {**params, "table": table}

        columns_rendered = list(map(lambda col: col.render(**params_merged), columns))
        self._column_names = list(map(lambda col: col.name, columns_rendered))

        self._add_columns = list(
            map(
                lambda col: Templates.add_column.render(table=table, column=col),
                columns_rendered,
            )
        )
        self.scripts["Add Columns"] = "\n\n".join(self._add_columns)

        self.scripts["Select"] = self._select = sql_environment.render(
            select, **params_merged
        )
        self.scripts["Update"] = self._update_table = Templates.update_table.render(
            table=table, columns=columns_rendered, id_fields=id_fields
        )

    def run(self, conn: Connection):
        for add_column in self._add_columns:
            conn.execute(add_column)

        cursor = conn.cursor(with_dict_factory)
        for input_row in cursor.execute(self._select):
            output_row = self._fn(**input_row)
            conn.execute(self._update_table, output_row)

    def delete(self, conn: Connection):
        console.log("[red]DROP COLUMN is unsupported in sqlite")

    def exists(self, conn: Connection):
        return columns_exist(conn, self._table, self._column_names)