from sqlite3 import Connection
from sqlglot.expressions import Table

from lib.templates import SqlEnvironment
from .base import Task


class Templates:
    drop_table = SqlEnvironment.default.from_string("DROP TABLE {{table}};")


class CreateTableSql(Task):
    def __init__(self, table: Table, sql: str, params: dict = {}) -> None:
        super().__init__()

        self.scripts["Main"] = self._sql = SqlEnvironment.default.from_string(
            sql
        ).render(table=table, **params)
        self.scripts["Drop Table"] = self._drop_table = Templates.drop_table.render(
            table=table
        )

    def run(self, conn: Connection):
        conn.executescript(self._sql)

    def delete(self, conn: Connection):
        conn.execute(self._drop_table)
