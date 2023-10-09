from pathlib import Path
import sqlite3

from lib import (
    table,
    MapToNewTable,
    ValueColumn,
    CreateTableSql,
    AddColumnsSql,
    with_args,
    placeholder,
    SqlEnvironment,
)
from parsers.scival import (
    parse_record_folder,
    IncitesCsvConfig,
    split_column,
    split_columns_many,
)

TABLE_records = table("records")
TABLE_sources = table("sources")
TABLE_record_ids = table("record_ids")
TABLE_record_authors = table("record_authors")
TABLE_record_affiliation = table("record_affiliation")
TABLE_record_category = table("record_category")
TABLE_record_topics = table("record_topics")
TABLE_record_metrics = table("record_metrics")

id_slice = 1

Path("tmp.scival.db").unlink(missing_ok=True)
with sqlite3.connect("tmp.scival.db") as conn:
    MapToNewTable(
        table=TABLE_records,
        columns=[
            "id_record INTEGER PRIMARY KEY AUTOINCREMENT",
            ValueColumn("filename", "TEXT"),
            ValueColumn("sgr", "TEXT"),
            ValueColumn("eid", "TEXT"),
            ValueColumn("citations", "TEXT"),
            ValueColumn("year", "TEXT"),
            ValueColumn("num_source", "TEXT", placeholder("source_id")),
            ValueColumn("doi", "TEXT"),
            ValueColumn("source_type", "TEXT"),
            ValueColumn("scopus_source_title", "TEXT"),
            ValueColumn("publication_type", "TEXT"),
            ValueColumn("number_of_authors", "TEXT"),
            ValueColumn("issn", "TEXT"),
            ValueColumn("asjc", "TEXT"),
            ValueColumn("scopus_author_ids", "TEXT"),
            ValueColumn("scopus_affiliation_ids", "TEXT"),
            ValueColumn("institutions", "TEXT"),
            ValueColumn("scopus_affiliation_names", "TEXT"),
            ValueColumn("country", "TEXT"),
            ValueColumn("topic_number", "TEXT"),
            ValueColumn("topic_name", "TEXT"),
            ValueColumn("topic_prominence_percentile", "TEXT"),
            ValueColumn("topic_cluster_number", "TEXT"),
            ValueColumn("topic_cluster_name", "TEXT"),
            ValueColumn("topic_cluster_prominence_percentile", "TEXT"),
            ValueColumn("field_weighted_view_impact", "TEXT"),
            ValueColumn("field_weighted_citation_impact", "TEXT"),
            ValueColumn("views", "TEXT"),
            ValueColumn("outputs_in_top_citation_percentiles_per_percentile", "TEXT"),
            ValueColumn(
                "field_weighted_outputs_in_top_citation_percentiles_per_percentile",
                "TEXT",
            ),
            ValueColumn("snip", "TEXT"),
            ValueColumn("cite_score", "TEXT"),
            ValueColumn("sjr", "TEXT"),
            ValueColumn("snip_percentile", "TEXT"),
            ValueColumn("cite_score_percentile", "TEXT"),
            ValueColumn("sjr_percentile", "TEXT"),
            ValueColumn("QS_Subject_field_name", "TEXT"),
            ValueColumn("THE_field_name", "TEXT"),
        ],
        fn=with_args(
            parse_record_folder,
            folder=Path("../scival_kedro/страны/"),
            config=IncitesCsvConfig.from_json(Path("./страны_config.json").read_text()),
        ),
    ).run(conn)
    CreateTableSql(
        table=TABLE_sources,
        sql=Path("./parsers/scival/csml_source.sql").read_text(),
        params={"records": TABLE_records},
    ).run(conn)
    AddColumnsSql(
        table=TABLE_records,
        sql=Path("./parsers/scival/map_source_id.sql").read_text(),
        params={"sources": TABLE_sources},
    ).run(conn)

    CreateTableSql(
        table=TABLE_record_ids,
        sql=Path("./parsers/scival/record_ids.sql").read_text(),
        params={"records": TABLE_records},
    ).run(conn)
    MapToNewTable(
        source_table=TABLE_records,
        select="SELECT scopus_author_ids AS auid, id_record, eid FROM {{source}}",
        table=TABLE_record_authors,
        columns=[
            "id_record_author INTEGER PRIMARY KEY AUTOINCREMENT",
            ValueColumn("auid", "TEXT"),
            ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
            ValueColumn("eid", "TEXT"),
        ],
        fn=split_column("auid"),
    ).run(conn)
    MapToNewTable(
        source_table=TABLE_records,
        select="SELECT scopus_affiliation_ids AS afid, id_record, eid FROM {{source}}",
        table=TABLE_record_affiliation,
        columns=[
            "id_record_affiliation INTEGER PRIMARY KEY AUTOINCREMENT",
            ValueColumn("afid", "TEXT"),
            ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
            ValueColumn("eid", "TEXT"),
        ],
        fn=split_column("afid"),
    ).run(conn)

    MapToNewTable(
        source_table=TABLE_records,
        select="""\
            SELECT
                asjc,
                institutions,
                scopus_affiliation_ids,
                scopus_affiliation_names,
                QS_Subject_field_name,
                THE_field_name,
                filename,
                id_record
            FROM {{source}}""",
        table=TABLE_record_category,
        columns=[
            ValueColumn("field_name", "TEXT"),
            ValueColumn("value_category", "TEXT"),
            ValueColumn("filename", "TEXT"),
            ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
        ],
        fn=split_columns_many(
            "asjc",
            "institutions",
            "scopus_affiliation_ids",
            "scopus_affiliation_names",
            "QS_Subject_field_name",
            "THE_field_name",
        ),
    ).run(conn)
    AddColumnsSql(
        sql=Path("./parsers/scival/map_type_category.sql").read_text(),
        table=TABLE_record_category,
        params={
            "mapping": {
                "asjc": 5,
                "institutions": 25,
                "scopus_affiliation_ids": 9,
                "scopus_affiliation_names": 24,
                "QS_Subject_field_name": "QS_Subject_field_name",
                "THE_field_name": "THE_field_name",
            }
        },
    ).run(conn)
    CreateTableSql(
        table=TABLE_record_topics,
        sql=Path("./parsers/scival/record_topics.sql").read_text(),
        params={"records": TABLE_records},
    ).run(conn)
    CreateTableSql(
        table=TABLE_record_metrics,
        sql=Path("./parsers/scival/record_metrics.sql").read_text(),
        params={"records": TABLE_records},
    ).run(conn)

Path("scival.db").unlink(missing_ok=True)
with sqlite3.connect("scival.db") as conn:
    for sql_path in [
        "./schema/csml_source.sql",
        "./schema/pure_record.sql",
        "./schema/csml_record_topics.sql",
        "./schema/csml_record_metrics.sql",
        "./schema/csml_record_category_metrics.sql",
        "./schema/csml_category_classification_code.sql",
    ]:
        print("Running", sql_path)
        conn.executescript(Path(sql_path).read_text())

    conn.execute("ATTACH DATABASE 'tmp.scival.db' AS 'tmp'")
    conn.executescript(
        SqlEnvironment.default.from_string(Path("./from_tmp.sql").read_text()).render(
            slice=0
        )
    )
    conn.executescript(
        SqlEnvironment.default.from_string(
            Path("./postprocess.sql").read_text()
        ).render(slice=0)
    )
