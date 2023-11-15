from sqlite3 import Connection, OperationalError
from typing import Optional
from typing_extensions import Callable
from returns.maybe import Maybe
from sqlglot.expressions import Table

from lib.checks import columns_exist
from lib.console import console, track
from ..templates import ValueColumn, IdColumn, sql_environment, identifier
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
    drop_column = sql_environment.from_string(
        """\
        ALTER TABLE {{table}}
        DROP COLUMN {{ column.identifier() }};
        """
    )


class MapToNewColumns(Task):
    def __init__(
        self,
        select: str,
        table: Table,
        columns: list[ValueColumn],
        fn: Callable[..., dict],
        id_fields: Optional[list[IdColumn]] = None,
        is_done_column: Optional[str] = None,
        params: dict = {},
    ) -> None:
        super().__init__()

        self._fn = fn
        self._table = table

        if is_done_column:
            columns.append(ValueColumn(is_done_column, "BOOL DEFAULT FALSE", True))
        self._commit_each = bool(is_done_column)

        params_merged = {
            **params,
            "table": table,
            "is_done": Maybe.from_optional(is_done_column)
            .map(identifier)
            .value_or(None),
        }

        columns_rendered = list(map(lambda col: col.render(**params_merged), columns))
        self._column_names = list(map(lambda col: col.name, columns_rendered))

        if id_fields is None:
            id_field_names = getattr(fn, "id_fields", None)
            assert (
                id_field_names
            ), "Couldn't infer id_fields for mapping function, please provide explicitly"

            id_fields = list(map(IdColumn, id_field_names))

        self._add_columns = list(
            map(
                lambda col: Templates.add_column.render(table=table, column=col),
                columns_rendered,
            )
        )
        self._drop_columns = list(
            map(
                lambda col: Templates.drop_column.render(table=table, column=col),
                columns_rendered,
            )
        )
        self.scripts["Add Columns"] = "\n\n".join(self._add_columns)
        self.scripts["Drop Columns"] = "\n\n".join(self._drop_columns)

        self.scripts["Select"] = self._select = sql_environment.render(
            select, **params_merged
        )
        self.scripts["Update"] = self._update_table = Templates.update_table.render(
            table=table, columns=columns_rendered, id_fields=id_fields
        )

    def run(self, conn: Connection):
        for add_column in self._add_columns:
            try:
                conn.execute(add_column)
            except OperationalError:
                pass

        cursor = conn.cursor(with_dict_factory)
        for input_row in track(
            cursor.execute(self._select).fetchall(), description="Task progress..."
        ):
            output_row = self._fn(**input_row)
            conn.execute(self._update_table, output_row)

            if self._commit_each:
                conn.commit()

    def delete(self, conn: Connection):
        try:
            for drop_column in self._drop_columns:
                conn.execute(drop_column)
        except OperationalError as err:
            # DROP COLUMN may not be supported in all sqlite versions
            console.log(err)

    def exists(self, conn: Connection):
        return columns_exist(conn, self._table, self._column_names) and (
            not self._commit_each
            # If this is a resumable task, check if inputs are empty
            or conn.execute(self._select).fetchone() is None
        )
