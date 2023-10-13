from sqlite3 import Connection
from typing import Iterable
from sqlglot.expressions import Table

from .templates import default_environment, identifier


def table_exists(conn: Connection, table: Table) -> bool:
    return (
        conn.execute(
            default_environment.render(
                """\
                SELECT 1 FROM {{master}}
                WHERE type = 'table' AND name = ?""",
                master=Table(
                    this=identifier("sqlite_master"), db=table.args.get("db", None)
                ),
            ),
            [table.name],
        ).fetchone()
        is not None
    )


def column_exists(conn: Connection, table: Table, column: str) -> bool:
    return (
        conn.execute(
            default_environment.render(
                """\
                SELECT 1 FROM {% if schema %}{{ schema }}.{% endif %}pragma_table_info(:table)
                WHERE name = :column""",
                schema=table.args.get("db", None),
            ),
            {"table": table.name, "column": column},
        ).fetchone()
        is not None
    )


def columns_exist(conn: Connection, table: Table, columns: Iterable[str]) -> bool:
    for column in columns:
        if not column_exists(conn, table, column):
            return False
    return True
