from pathlib import Path
import sqlite3

from parsers.internalorgs import parse_internalorg_folder, parse_internalorg_ids
from templates import table, ValueColumn
from tasks import MapToNewTable

TABLE_internalorgs = table("internalorgs")
TABLE_internalorg_ids = table("internalorg_ids")


def with_args(fn, **add_kwargs):
    def wrapper(**kwargs):
        yield from fn(**kwargs, **add_kwargs)

    return wrapper


def pop_id_fields(fn, *id_fields: str):
    def wrapper(**kwargs):
        id_values = {name: kwargs.pop(name) for name in id_fields}
        for row in fn(**kwargs):
            yield {**id_values, **row}

    return wrapper


Path("pure.db").unlink(missing_ok=True)
with sqlite3.connect("pure.db") as conn:
    MapToNewTable(
        table=TABLE_internalorgs,
        columns=[
            "org_id INTEGER PRIMARY KEY AUTOINCREMENT",
            ValueColumn("uuid", "TEXT"),
            ValueColumn("parent_uuid", "TEXT"),
            ValueColumn("kind_pure_org", "TEXT"),
            ValueColumn("name_pure_org", "TEXT"),
            ValueColumn("name_pure_org_eng", "TEXT"),
            ValueColumn("ids", "TEXT"),
        ],
        fn=with_args(
            parse_internalorg_folder, folder=Path("../rez_internalorg_20220926/")
        ),
    ).run(conn)
    MapToNewTable(
        select="SELECT org_id, ids AS xml FROM {{source}}",
        table=TABLE_internalorg_ids,
        columns=[
            ValueColumn("org_id", "INT REFERENCES {{source}}"),
            ValueColumn("id_value", "TEXT"),
            ValueColumn("type_name", "TEXT"),
        ],
        fn=pop_id_fields(parse_internalorg_ids, "org_id"),
        params={"source": TABLE_internalorgs},
    ).run(conn)
