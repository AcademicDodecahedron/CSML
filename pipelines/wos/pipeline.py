from returns.maybe import Maybe

from lib import (
    TaskTree,
    table,
    compose,
    compose_one,
    rename_output,
    pop_id_fields,
    add_to_input,
    MapToNewTable,
    ValueColumn,
    AddColumnsSql,
    Column,
    CreateTableSql,
    MapToNewColumns,
)
from lib.utils import folder
from pipelines.glob import load_files_glob
from .nodes.loaders import load_wos, load_incites
from .nodes.record_authors import split_wos_authors
from .nodes.rel_affiliations import parse_rel_affiliations, split_address
from .nodes.record_topics import split_topic
from .nodes.record_category import split_category
from .nodes.database_record import make_database_record
from .fields import WOS_COLUMNS, INCITES_COLUMNS, normalize_name
from .config import WosConfig


def create_tasks(config: WosConfig) -> TaskTree:
    table_wos = table("wos")
    table_incites = table("incites")
    table_records = table("records")
    table_database_record = table("database_record")
    table_record_topics = table("record_topics")
    table_record_metrics = table("record_metrics")
    table_record_authors = table("record_authors")
    table_rel_affiliations_raw = table("rel_affiliations_raw")
    table_record_affiliations = table("record_affiliations")
    table_rel_affiliations = table("rel_affiliations")

    table_filename = table("filename_association")
    table_category_split = table("category_split")
    table_record_category = table("record_category")

    return {
        "wos": {
            "load": MapToNewTable(
                table=table_wos,
                columns=[ValueColumn("filename", "TEXT"), *WOS_COLUMNS],
                fn=load_files_glob(config.wos.glob, load_wos),
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
                fn=compose(
                    load_files_glob(
                        Maybe.from_optional(config.incites)
                        .map(lambda incites: incites.glob)
                        .value_or(None),
                        load_incites,
                    ),
                    rename_output(normalize_name),
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
                sql=folder().joinpath("./nodes/records.sql").read_text(),
                params={"wos": table_wos, "incites": table_incites},
            ),
            "filename": CreateTableSql(
                table=table_filename,
                sql=folder().joinpath("./nodes/filename_association.sql").read_text(),
                params={
                    "records": table_records,
                    "wos": table_wos,
                    "incites": table_incites,
                },
            ),
        },
        "database_record": MapToNewTable(
            source_table=table_records,
            select="SELECT id_record, ut, database_we AS we, database_pm AS pm FROM {{source}}",
            table=table_database_record,
            columns=[
                ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                ValueColumn("name_database", "TEXT"),
                ValueColumn("num_record_in_database", "TEXT"),
            ],
            fn=compose(make_database_record, pop_id_fields("id_record")),
        ),
        "record_topics": {
            "create": CreateTableSql(
                table=table_record_topics,
                sql=folder().joinpath("./nodes/record_topics.sql").read_text(),
                params={"records": table_records, "incites": table_incites},
            ),
            "split": MapToNewColumns(
                table=table_record_topics,
                select="SELECT id_record_topic, research_area AS topic FROM {{table}}",
                columns=[
                    ValueColumn("num_topics", "TEXT"),
                    ValueColumn("name_topics", "TEXT"),
                ],
                fn=compose_one(split_topic, pop_id_fields("id_record_topic")),
            ),
        },
        "record_metrics": CreateTableSql(
            table=table_record_metrics,
            sql=folder().joinpath("./nodes/record_metrics.sql").read_text(),
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
            fn=compose(split_wos_authors, pop_id_fields("id_record")),
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
            fn=compose(parse_rel_affiliations, pop_id_fields("id_record")),
        ),
        "record_affiliations": {
            "create": CreateTableSql(
                table=table_record_affiliations,
                sql=folder().joinpath("./nodes/record_affiliations.sql").read_text(),
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
                    ValueColumn("city", "TEXT"),
                    ValueColumn("index", "TEXT"),
                ],
                fn=compose_one(
                    split_address,
                    add_to_input(config=config.address),
                    pop_id_fields("id_record_affiliation"),
                ),
            ),
            "connect": AddColumnsSql(
                table=table_rel_affiliations_raw,
                sql=folder()
                .joinpath("./nodes/record_affiliations_connect.sql")
                .read_text(),
                params={"record_affiliations": table_record_affiliations},
            ),
        },
        "rel_affiliations": CreateTableSql(
            table=table_rel_affiliations,
            sql=folder().joinpath("./nodes/rel_affiliations_full.sql").read_text(),
            params={
                "raw": table_rel_affiliations_raw,
                "authors": table_record_authors,
                "affiliations": table_record_affiliations,
            },
        ),
        "record_category": {
            "split": MapToNewTable(
                source_table=table_records,
                select="SELECT id_record, category_wc, category_sc FROM {{source}}",
                table=table_category_split,
                columns=[
                    ValueColumn("id_record", "INT REFERENCES {{records}}(id_record)"),
                    ValueColumn("field_name", "TEXT"),
                    ValueColumn("type_category", "TEXT"),
                    ValueColumn("value_category", "TEXT"),
                ],
                fn=split_category({"category_wc": 3, "category_sc": 4}),
                params={"records": table_records},
            ),
            "combine": CreateTableSql(
                table=table_record_category,
                sql=folder().joinpath("./nodes/record_category.sql").read_text(),
                params={
                    "records": table_records,
                    "split": table_category_split,
                    "filename": table_filename,
                    "affiliations": table_record_affiliations,
                },
            ),
        },
    }
