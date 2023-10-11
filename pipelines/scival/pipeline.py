from pathlib import Path

from lib import (
    table,
    identifier,
    MapToNewTable,
    CreateTableSql,
    AddColumnsSql,
    ValueColumn,
    Column,
    with_args,
)
from .config import ScivalConfig
from .nodes.load_records import load_records_csv_or_folder
from .nodes.split_column import split_column, split_categories


def create_tasks(config: ScivalConfig):
    table_records_raw = table("records_raw")
    table_records = table("records")
    table_sources = table("sources")
    table_record_ids = table("record_ids")
    table_record_authors = table("record_authors")
    table_record_affiliation = table("record_affiliation")
    table_category_split = table("category_split")
    table_record_category = table("record_category")
    table_record_topics = table("record_topics")
    table_record_metrics = table("record_metrics")

    record_columns = [
        *map(lambda name: ValueColumn(name, "TEXT").render(), config.fields.values()),
    ]

    parent_folder = Path(__file__).parent

    return {
        "records": {
            "load": MapToNewTable(
                table=table_records_raw,
                columns=[
                    ValueColumn("filename", "TEXT"),
                    *record_columns,
                ],
                fn=with_args(
                    load_records_csv_or_folder,
                    path=config.path,
                    header_length=config.header_length,
                    mapping=config.fields,
                ),
            ),
            "dedupe": CreateTableSql(
                table=table_records,
                sql=parent_folder.joinpath("./nodes/dedupe_records.sql").read_text(),
                params={"raw": table_records_raw, "record_columns": record_columns},
            ),
            "sgr": AddColumnsSql(
                table=table_records,
                columns=[Column("sgr", "TEXT")],
                sql="""\
                UPDATE {{table}}
                SET sgr = REPLACE(eid, '2-s2.0-', '')""",
            ),
        },
        "sources": {
            "create": CreateTableSql(
                table=table_sources,
                sql=parent_folder.joinpath("./nodes/csml_source.sql").read_text(),
                params={"records": table_records},
            ),
            "connect": AddColumnsSql(
                table=table_records,
                sql=parent_folder.joinpath("./nodes/map_source_id.sql").read_text(),
                params={"sources": table_sources},
            ),
        },
        "record_ids": CreateTableSql(
            table=table_record_ids,
            sql=parent_folder.joinpath("./nodes/record_ids.sql").read_text(),
            params={"records": table_records},
        ),
        "record_authors": MapToNewTable(
            source_table=table_records,
            select="SELECT scopus_author_ids AS auid, id_record, eid FROM {{source}}",
            table=table_record_authors,
            columns=[
                "id_record_author INTEGER PRIMARY KEY AUTOINCREMENT",
                ValueColumn("auid", "TEXT"),
                ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                ValueColumn("eid", "TEXT"),
            ],
            fn=split_column("auid"),
        ),
        "record_affiliation": MapToNewTable(
            source_table=table_records,
            select="SELECT scopus_affiliation_ids AS afid, id_record, eid FROM {{source}}",
            table=table_record_affiliation,
            columns=[
                "id_record_affiliation INTEGER PRIMARY KEY AUTOINCREMENT",
                ValueColumn("afid", "TEXT"),
                ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                ValueColumn("eid", "TEXT"),
            ],
            fn=split_column("afid"),
        ),
        "record_category": {
            "split": MapToNewTable(
                source_table=table_records,
                select="""\
                SELECT
                id_record,
                {{ select | sqljoin(',\\n') }}
                FROM {{source}}""",
                table=table_category_split,
                columns=[
                    ValueColumn("field_name", "TEXT"),
                    ValueColumn("value_category", "TEXT"),
                    ValueColumn("type_category", "INT"),
                    ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                ],
                fn=split_categories(config.category_mapping),
                params={"select": map(identifier, config.category_mapping.keys())},
            ),
            "add_filename": CreateTableSql(
                table=table_record_category,
                sql=parent_folder.joinpath("./nodes/category_filename.sql").read_text(),
                params={
                    "split": table_category_split,
                    "records": table_records,
                    "records_raw": table_records_raw,
                },
            ),
        },
        "record_topics": CreateTableSql(
            table=table_record_topics,
            sql=parent_folder.joinpath("./nodes/record_topics.sql").read_text(),
            params={"records": table_records},
        ),
        "record_metrics": CreateTableSql(
            table=table_record_metrics,
            sql=parent_folder.joinpath("./nodes/record_metrics.sql").read_text(),
            params={"records": table_records},
        ),
    }
