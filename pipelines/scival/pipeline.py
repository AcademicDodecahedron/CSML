from lib import (
    TaskTree,
    table,
    MapToNewTable,
    CreateTableSql,
    AddColumnsSql,
    ValueColumn,
    ValueColumnRendered,
    Column,
    compose,
    add_to_input,
)
from returns.maybe import Maybe
from lib.utils import folder
from pipelines.glob import load_files_glob
from .config import ScivalConfig
from .nodes.load_records import load_records_csv
from .nodes.split_column import split_column, split_categories


def create_tasks(config: ScivalConfig) -> TaskTree:
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

    table_work_slice_staff = table("work_slice_staff")

    record_columns = list(
        map(lambda name: ValueColumnRendered(name, "TEXT"), config.fields.values())
    )

    return {
        "records": {
            "load": MapToNewTable(
                table=table_records_raw,
                columns=[
                    ValueColumn("filename", "TEXT"),
                    *record_columns,
                ],
                fn=load_files_glob(
                    config.glob,
                    compose(
                        load_records_csv,
                        add_to_input(
                            header_length=config.header_length,
                            mapping=config.fields,
                        ),
                    ),
                ),
            ),
            "dedupe": CreateTableSql(
                table=table_records,
                sql=folder().joinpath("./nodes/dedupe_records.sql").read_text(),
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
                sql=folder().joinpath("./nodes/csml_source.sql").read_text(),
                params={"records": table_records},
            ),
            "connect": AddColumnsSql(
                table=table_records,
                sql=folder().joinpath("./nodes/map_source_id.sql").read_text(),
                params={"sources": table_sources},
            ),
        },
        "record_ids": CreateTableSql(
            table=table_record_ids,
            sql=folder().joinpath("./nodes/record_ids.sql").read_text(),
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
                {% set sep = sqljoiner(',\\n') -%}
                SELECT
                {{ sep() }}id_record
                {%- for name in select %}{{ sep() }}{{ name | identifier }}{% endfor %}
                FROM {{source}}""",
                table=table_category_split,
                columns=[
                    ValueColumn("field_name", "TEXT"),
                    ValueColumn("value_category", "TEXT"),
                    ValueColumn("type_category", "INT"),
                    ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                ],
                fn=split_categories(config.category_mapping),
                params={"select": config.category_mapping.keys()},
            ),
            "add_filename": CreateTableSql(
                table=table_record_category,
                sql=folder().joinpath("./nodes/category_filename.sql").read_text(),
                params={
                    "split": table_category_split,
                    "records": table_records,
                    "records_raw": table_records_raw,
                },
            ),
        },
        "record_topics": CreateTableSql(
            table=table_record_topics,
            sql=folder().joinpath("./nodes/record_topics.sql").read_text(),
            params={"records": table_records},
        ),
        "record_metrics": CreateTableSql(
            table=table_record_metrics,
            sql=folder().joinpath("./nodes/record_metrics.sql").read_text(),
            params={"records": table_records},
        ),
        "work_slice_staff": MapToNewTable(
            table=table_work_slice_staff,
            columns=[
                ValueColumn("ScopusName", "TEXT"),
                ValueColumn("ScopusAff", "TEXT"),
                ValueColumn("ScopusID", "TEXT"),
            ],
            fn=load_files_glob(
                Maybe.from_optional(config.authors)
                .map(lambda authors: authors.glob)
                .value_or(None),
                compose(
                    load_records_csv,
                    add_to_input(
                        header_length=Maybe.from_optional(config.authors)
                        .map(lambda authors: authors.header_length)
                        .value_or(None),
                        mapping=Maybe.from_optional(config.authors)
                        .map(lambda authors: authors.fields)
                        .value_or(None),
                    ),
                ),
            ),
        ),
    }
