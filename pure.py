from pathlib import Path
import sqlite3

from templates import table, ValueColumn
from tasks import MapToNewTable
from parsers.internalorgs import parse_internalorg_folder, parse_internalorg_ids
from parsers.externalorgs import parse_externalorg_folder, parse_externalorg_ids
from parsers.internalpersons import (
    parse_internalperson_folder,
    parse_internalperson_ids,
    parse_internalperson_variants,
    parse_internalperson_associations,
)

TABLE_internalorgs = table("internalorgs")
TABLE_internalorg_ids = table("internalorg_ids")
TABLE_externalorgs = table("externalorgs")
TABLE_externalorg_ids = table("externalorg_ids")
TABLE_internalpersons = table("internalpersons")
TABLE_internalperson_ids = table("internalperson_ids")
TABLE_internalperson_variants = table("internalperson_name_variants")
TABLE_internalperson_associations = table("internalperson_associations")


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

    MapToNewTable(
        table=TABLE_externalorgs,
        columns=[
            "org_id INTEGER PRIMARY KEY AUTOINCREMENT",
            ValueColumn("uuid", "TEXT"),
            ValueColumn("parent_uuid", "TEXT"),
            ValueColumn("kind_pure_org", "TEXT"),
            ValueColumn("name_pure_org", "TEXT"),
            ValueColumn("name_pure_org_eng", "TEXT"),
            ValueColumn("country_org", "TEXT"),
            ValueColumn("city_org", "TEXT"),
            ValueColumn("ids", "TEXT"),
        ],
        fn=with_args(
            parse_externalorg_folder, folder=Path("../rez_externalorg_20220926/")
        ),
    ).run(conn)
    MapToNewTable(
        select="SELECT org_id, ids AS xml FROM {{source}} WHERE ids IS NOT NULL",
        table=TABLE_externalorg_ids,
        columns=[
            ValueColumn("org_id", "INT REFERENCES {{source}}"),
            ValueColumn("id_value", "TEXT"),
            ValueColumn("type_name", "TEXT"),
        ],
        fn=pop_id_fields(parse_externalorg_ids, "org_id"),
        params={"source": TABLE_externalorgs},
    ).run(conn)

    MapToNewTable(
        table=TABLE_internalpersons,
        columns=[
            "person_id INTEGER PRIMARY KEY AUTOINCREMENT",
            ValueColumn("uuid", "TEXT"),
            ValueColumn("first_name", "TEXT"),
            ValueColumn("last_name", "TEXT"),
            ValueColumn("date_of_birth", "TEXT"),
            ValueColumn("year_of_birth", "INT"),
            ValueColumn("orcid", "TEXT"),
            ValueColumn("year", "INT"),
            ValueColumn("education", "TEXT"),
            ValueColumn("qualification", "TEXT"),
            ValueColumn("name_variants", "TEXT"),
            ValueColumn("ids", "TEXT"),
            ValueColumn("associations", "TEXT"),
        ],
        fn=with_args(
            parse_internalperson_folder,
            folder=Path("../pure_internalperson_data/rez_internalperson_20220926"),
        ),
    ).run(conn)
    MapToNewTable(
        select="SELECT person_id, ids AS xml FROM {{source}} WHERE ids IS NOT NULL",
        table=TABLE_internalperson_ids,
        columns=[
            ValueColumn("person_id", "INT REFERENCES {{source}}"),
            ValueColumn("id_value", "TEXT"),
            ValueColumn("type_name", "TEXT"),
        ],
        fn=pop_id_fields(parse_internalperson_ids, "person_id"),
        params={"source": TABLE_internalpersons},
    ).run(conn)
    MapToNewTable(
        select="SELECT person_id, name_variants AS xml FROM {{source}} WHERE name_variants IS NOT NULL",
        table=TABLE_internalperson_variants,
        columns=[
            ValueColumn("person_id", "INT REFERENCES {{source}}"),
            ValueColumn("firstname", "TEXT"),
            ValueColumn("lastname", "TEXT"),
            ValueColumn("typename", "TEXT"),
        ],
        fn=pop_id_fields(parse_internalperson_variants, "person_id"),
        params={"source": TABLE_internalpersons},
    ).run(conn)
    MapToNewTable(
        select="SELECT person_id, associations AS xml FROM {{source}} WHERE associations IS NOT NULL",
        table=TABLE_internalperson_associations,
        columns=[
            ValueColumn("person_id", "INT REFERENCES {{source}}"),
            ValueColumn("uuid_org", "TEXT"),
            ValueColumn("type_work", "TEXT"),
            ValueColumn("start_date", "TEXT"),
            ValueColumn("end_date", "TEXT"),
            ValueColumn("is_primary_asociation", "INT"),
            ValueColumn("job_description", "TEXT"),
            ValueColumn("job_name", "TEXT"),
        ],
        fn=pop_id_fields(parse_internalperson_associations, "person_id"),
        params={"source": TABLE_internalpersons},
    ).run(conn)
