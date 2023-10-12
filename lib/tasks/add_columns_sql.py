from sqlite3 import Connection
from typing import Optional
from sqlglot.expressions import Table

from .base import Task
from lib.templates import Column, SqlEnvironment


class Templates:
    add_columns = SqlEnvironment.default.from_string(
        """\
        {% set sep = joiner(',\\n    ') -%}
        ALTER TABLE {{ table }}
        {% for column in columns -%}
        {{ sep() | sql }}ADD COLUMN {{ column.definition() }}
        {%- endfor %};"""
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

        params_joined = {**params, "table": table}
        script_module = SqlEnvironment.default.from_string(sql).make_module(
            params_joined
        )

        if columns is None:
            # Get columns variable from template: {% set columns = [...] %}
            columns = getattr(script_module, "columns", None)
            if columns is None:
                raise ValueError("Columns not specified")

        columns_rendered = list(map(lambda col: col.render(**params_joined), columns))
        self.scripts["Add Columns"] = self._add_columns = Templates.add_columns.render(
            table=table, columns=columns_rendered
        )
        self.scripts["Main"] = self._sql = str(script_module)

    def run(self, conn: Connection):
        conn.execute(self._add_columns)
        conn.executescript(self._sql)

    def delete(self, conn: Connection):
        print("DROP COLUMN is unsupported in sqlite")
