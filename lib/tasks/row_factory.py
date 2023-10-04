from sqlite3 import Connection, Cursor
from typing import Any, Iterable


def dict_factory(cursor: Cursor, row: Iterable[Any]):
    fields = [column[0] for column in cursor.description]
    return {key: value for key, value in zip(fields, row)}


def with_dict_factory(conn: Connection) -> Cursor:
    curs = conn.cursor()
    curs.row_factory = dict_factory
    return curs
