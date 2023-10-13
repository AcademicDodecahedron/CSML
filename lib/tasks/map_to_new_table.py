from sqlite3 import Connection
from typing import Callable, Iterator, Optional
from sqlglot.expressions import Table

from lib.templates import ValueColumn, ValueColumnRendered, default_environment, Sql
from lib.checks import table_exists
from .base import Task
from .row_factory import with_dict_factory


class Templates:
    create_table = default_environment.from_string(
        """\
        CREATE TABLE {{table}}(
            {{ definitions | sqljoin(',\\n    ') }}
        );
        """
    )
    insert = default_environment.from_string(
        """\
        {% set sep = joiner(',\\n    ') -%}
        INSERT INTO {{table}}(
            {% for column in columns -%}
            {{ sep() | sql }}{{ column.identifier() }}
            {%- endfor %}
        )
        VALUES (
            {{ columns | sqljoin(',\\n    ', attribute='value') }}
        );
        """
    )
    drop_table = default_environment.from_string("DROP TABLE {{table}};")


class MapToNewTable(Task):
    def __init__(
        self,
        table: Table,
        columns: list[str | ValueColumn | ValueColumnRendered],
        fn: Callable[..., Iterator[dict]],
        select: Optional[str] = None,
        source_table: Optional[Table] = None,
        params: dict = {},
    ) -> None:
        super().__init__()

        self._table = table
        params_merged = {**params, "table": table, "source": source_table}

        definitions, insert_columns = [], []
        for column in columns:
            if isinstance(column, str):
                rendered = default_environment.render(column, **params_merged)

                definitions.append(Sql(rendered))
            else:
                rendered = (
                    column.render(**params_merged)
                    if isinstance(column, ValueColumn)
                    else column
                )

                insert_columns.append(rendered)
                definitions.append(rendered.definition())

        if select:
            self.scripts["Select"] = self._select = default_environment.render(
                select, **params_merged
            )
        else:
            self._select = None

        self.scripts[
            "Create Table"
        ] = self._create_table = Templates.create_table.render(
            table=table,
            definitions=definitions,
        )
        self.scripts["Insert"] = self._insert = Templates.insert.render(
            table=table,
            columns=insert_columns,
        )
        self.scripts["Drop Table"] = self._drop_table = Templates.drop_table.render(
            table=table
        )

        self._fn = fn

    def run(self, conn: Connection):
        conn.execute(self._create_table)

        def process_input(**input_row):
            for output_row in self._fn(**input_row):
                conn.execute(self._insert, output_row)

        if self._select:
            cursor = conn.cursor(with_dict_factory)
            for input_row in cursor.execute(self._select):
                process_input(**input_row)
        else:
            process_input()

    def delete(self, conn: Connection):
        conn.execute(self._drop_table)

    def exists(self, conn: Connection):
        return table_exists(conn, self._table)
