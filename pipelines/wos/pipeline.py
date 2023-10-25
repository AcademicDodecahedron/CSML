from pathlib import Path
from typing import Literal, Optional
from pydantic import BaseModel

from lib import (
    TaskTree,
    table,
    rename_output,
    pop_id_fields,
    pop_id_fields_one,
    MapToNewTable,
    ValueColumn,
    AddColumnsSql,
    Column,
    CreateTableSql,
    MapToNewColumns,
    IdColumn,
)
from .nodes.loaders import load_files_glob, load_wos, load_incites
from .nodes.record_authors import split_wos_authors
from .nodes.rel_affiliations import parse_rel_affiliations, split_address
from .nodes.record_topics import split_topic
from .fields import WOS_COLUMNS, INCITES_COLUMNS, normalize_name

__dir__ = Path(__file__).parent


class WosConfig(BaseModel):
    type: Literal["wos"]
    wos_glob: str
    incites_glob: Optional[str]

    def create_tasks(self):
        return create_tasks(self)


def create_tasks(config: WosConfig) -> TaskTree:
    table_wos = table("wos")
    table_incites = table("incites")
    table_records = table("records")
    table_record_topics = table("record_topics")
    table_record_metrics = table("record_metrics")
    table_record_authors = table("record_authors")
    table_rel_affiliations_raw = table("rel_affiliations_raw")
    table_record_affiliations = table("record_affiliations")
    table_rel_affiliations = table("rel_affiliations")

    table_filename = table("filename_association")

    return {
        "wos": {
            "load": MapToNewTable(
                table=table_wos,
                columns=[ValueColumn("filename", "TEXT"), *WOS_COLUMNS],
                fn=load_files_glob(config.wos_glob, load_wos),
            ),
            "num_record": AddColumnsSql(
                table=table_wos,
                columns=[Column("num_record", "TEXT")],
                sql="""\
                UPDATE {{table}}
                SET num_record = REPLACE(UT, 'WOS:', '')""",
            ),
        },
        "incites": {
            "load": MapToNewTable(
                table=table_incites,
                columns=[
                    ValueColumn("filename", "TEXT"),
                    ValueColumn("footer__schema", "TEXT"),
                    *INCITES_COLUMNS,
                ],
                fn=rename_output(
                    load_files_glob(config.incites_glob, load_incites), normalize_name
                ),
            ),
            "num_record": AddColumnsSql(
                table=table_incites,
                columns=[Column("num_record", "TEXT")],
                sql="""\
                UPDATE {{table}}
                SET num_record = REPLACE(accession_number, 'WOS:', '')""",
            ),
        },
        "records": {
            "create": CreateTableSql(
                table=table_records,
                sql=__dir__.joinpath("./nodes/records.sql").read_text(),
                params={"wos": table_wos, "incites": table_incites},
            ),
            "filename": CreateTableSql(
                table=table_filename,
                sql=__dir__.joinpath("./nodes/filename_association.sql").read_text(),
                params={
                    "records": table_records,
                    "wos": table_wos,
                    "incites": table_incites,
                },
            ),
        },
        "record_topics": {
            "create": CreateTableSql(
                table=table_record_topics,
                sql=__dir__.joinpath("./nodes/record_topics.sql").read_text(),
                params={"records": table_records, "incites": table_incites},
            ),
            "split": MapToNewColumns(
                table=table_record_topics,
                select="SELECT id_record_topic, research_area AS topic FROM {{table}}",
                columns=[
                    ValueColumn("num_topics", "TEXT"),
                    ValueColumn("name_topics", "TEXT"),
                ],
                fn=pop_id_fields_one(split_topic, "id_record_topic"),
                id_fields=[IdColumn("id_record_topic")],
            ),
        },
        "record_metrics": CreateTableSql(
            table=table_record_metrics,
            sql=__dir__.joinpath("./nodes/record_metrics.sql").read_text(),
            params={"records": table_records, "incites": table_incites},
        ),
        "record_authors": MapToNewTable(
            source_table=table_records,
            select="""\
            SELECT
                id_record,
                author_au AS au,
                author_af AS af
            FROM {{source}} WHERE author_au IS NOT NULL""",
            table=table_record_authors,
            columns=[
                "id_record_author INTEGER PRIMARY KEY AUTOINCREMENT",
                ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                ValueColumn("full_name", "TEXT"),
                ValueColumn("first_name", "TEXT"),
                ValueColumn("last_name", "TEXT"),
            ],
            fn=pop_id_fields(split_wos_authors, "id_record"),
        ),
        "rel_affiliations_raw": MapToNewTable(
            source_table=table_records,
            select="""\
            SELECT id_record, affiliation_c1 AS c1
            FROM {{source}} WHERE affiliation_c1 IS NOT NULL""",
            table=table_rel_affiliations_raw,
            columns=[
                ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                ValueColumn("order", "INT"),
                ValueColumn("full_address", "TEXT"),
                ValueColumn("author_name", "TEXT"),
            ],
            fn=pop_id_fields(parse_rel_affiliations, "id_record"),
        ),
        "record_affiliations": {
            "create": CreateTableSql(
                table=table_record_affiliations,
                sql=__dir__.joinpath("./nodes/record_affiliations.sql").read_text(),
                params={
                    "rel_aff": table_rel_affiliations_raw,
                    "records": table_records,
                },
            ),
            "address": MapToNewColumns(
                table=table_record_affiliations,
                select="SELECT id_record_affiliation, full_address AS address FROM {{table}}",
                columns=[
                    ValueColumn("org_name", "TEXT"),
                    ValueColumn("country", "TEXT"),
                ],
                fn=pop_id_fields_one(split_address, "id_record_affiliation"),
                id_fields=[IdColumn("id_record_affiliation")],
            ),
            "connect": AddColumnsSql(
                table=table_rel_affiliations_raw,
                sql=__dir__.joinpath(
                    "./nodes/record_affiliations_connect.sql"
                ).read_text(),
                params={"record_affiliations": table_record_affiliations},
            ),
        },
        "rel_affiliations": CreateTableSql(
            table=table_rel_affiliations,
            sql=__dir__.joinpath("./nodes/rel_affiliations_full.sql").read_text(),
            params={
                "raw": table_rel_affiliations_raw,
                "authors": table_record_authors,
                "affiliations": table_record_affiliations,
            },
        ),
    }
