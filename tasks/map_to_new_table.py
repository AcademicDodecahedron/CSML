from sqlite3 import Connection
from typing import Callable, Iterator, Optional
from sqlglot.expressions import Table
from templates import ValueColumn, SqlEnvironment, Sql

from .base import Task
from .row_factory import with_dict_factory


class Templates:
    create_table = SqlEnvironment.default.from_string(
        """\
        CREATE TABLE {{table}}(
            {{ definitions | sqljoin(',\\n    ') }}
        );
        """
    )
    insert = SqlEnvironment.default.from_string(
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
    drop_table = SqlEnvironment.default.from_string("DROP TABLE {{table}};")


class MapToNewTable(Task):
    def __init__(
        self,
        table: Table,
        columns: list[str | ValueColumn],
        fn: Callable[..., Iterator[dict]],
        select: Optional[str] = None,
        params: dict = {},
    ) -> None:
        super().__init__()

        definitions, insert_columns = [], []
        for column in columns:
            if isinstance(column, ValueColumn):
                rendered = column.render(**params)

                insert_columns.append(rendered)
                definitions.append(rendered.definition())
            else:
                rendered = SqlEnvironment.default.from_string(column).render(**params)

                definitions.append(Sql(rendered))

        if select:
            self.scripts["Select"] = self._select = SqlEnvironment.default.from_string(
                select
            ).render(table=table, **params)
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
