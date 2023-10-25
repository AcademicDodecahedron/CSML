from sqlite3 import Connection
from typing import Optional
from sqlglot.expressions import Table

from .base import Task
from lib.templates import Column, sql_environment
from lib.checks import columns_exist
from lib.console import console


class Templates:
    add_column = sql_environment.from_string(
        """\
        ALTER TABLE {{table}}
        ADD COLUMN {{ column.definition() }};
        """
    )


class AddColumnsSql(Task):
    def __init__(
        self,
        sql: str,
        table: Table,
        columns: Optional[list[Column]] = None,
        params: dict = {},
    ) -> None:
        super().__init__()

        self._table = table
        params_joined = {**params, "table": table}
        script_module = sql_environment.from_string(sql).make_module(params_joined)

        if columns is None:
            # Get columns variable from template: {% set columns = [...] %}
            columns = getattr(script_module, "columns", None)
            if columns is None:
                raise ValueError("Columns not specified")

        columns_rendered = list(map(lambda col: col.render(**params_joined), columns))
        self._column_names = list(map(lambda col: col.name, columns_rendered))

        self._add_columns = list(
            map(
                lambda col: Templates.add_column.render(table=table, column=col),
                columns_rendered,
            )
        )
        self.scripts["Add Columns"] = "\n\n".join(self._add_columns)

        self.scripts["Main"] = self._sql = str(script_module)

    def run(self, conn: Connection):
        for add_column in self._add_columns:
            conn.execute(add_column)

        conn.executescript(self._sql)

    def delete(self, conn: Connection):
        console.log("[red]DROP COLUMN is unsupported in sqlite")

    def exists(self, conn: Connection):
        return columns_exist(conn, self._table, self._column_names)
