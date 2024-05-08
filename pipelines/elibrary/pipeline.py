from lib import (
    TaskTree,
    MapToNewTable,
    table,
    placeholder,
    ValueColumn,
    compose,
    add_to_input,
    pop_id_fields,
    CreateTableSql,
    Column,
)
from lib.tasks.add_columns_sql import AddColumnsSql
from lib.utils import folder

from pipelines.glob import load_files_glob
from .config import ElibraryConfig
from .nodes.upload_xml import upload_xml
from .nodes.parse_records import parse_records, parse_authors, parse_affiliations


def create_tasks(config: ElibraryConfig) -> TaskTree:
    table_xml = table("raw_xml")
    table_records = table("csml_record")
    table_categories = table("csml_record_category")
    table_authors_raw = table("authors_raw")
    table_authors = table("csml_record_author")
    table_rel_aff = table("csml_record_author_rel_affiliation")
    table_affiliations = table("csml_record_affiliation")

    return {
        "upload": MapToNewTable(
            table=table_xml,
            columns=[
                "xml_id INTEGER PRIMARY KEY",
                ValueColumn("filename", "TEXT", placeholder("xmlname")),
                ValueColumn("xml", "TEXT"),
            ],
            fn=load_files_glob(
                config.glob, compose(upload_xml, add_to_input(encoding=config.encoding))
            ),
        ),
        "records": MapToNewTable(
            source_table=table_xml,
            select="SELECT xml_id, xml FROM {{source}}",
            table=table_records,
            columns=[
                "id_record INTEGER PRIMARY KEY",
                ValueColumn("xml_id", "INT REFERENCES {{source}}"),
                ValueColumn("num_record", "TEXT"),
                ValueColumn("year_publ", "INT"),
                ValueColumn("cited_from_record", "INT"),
                ValueColumn("lang_document", "TEXT"),
                ValueColumn("document_type", "TEXT"),
                ValueColumn("source_type", "TEXT"),
                ValueColumn("source_title", "TEXT"),
                ValueColumn("publisher", "TEXT"),
                ValueColumn("source_country", "TEXT"),
                ValueColumn("doi", "TEXT"),
                ValueColumn("issn_norm", "TEXT"),
                ValueColumn("refs_count", "INT"),
                ValueColumn("keywords_count", "INT"),
                ValueColumn("authors_count", "INT"),
                ValueColumn("grnti", "TEXT"),
                ValueColumn("vak", "TEXT"),
                ValueColumn("rsci", "TEXT"),
                ValueColumn("wos", "TEXT"),
                ValueColumn("scopus", "TEXT"),
                ValueColumn("authors", "TEXT"),
            ],
            fn=compose(parse_records, pop_id_fields("xml_id")),
        ),
        "categories": CreateTableSql(
            table=table_categories,
            sql=folder().joinpath("./nodes/record_category.sql").read_text(),
            params={"records": table_records, "xmls": table_xml},
        ),
        "authors": {
            "parse": MapToNewTable(
                source_table=table_records,
                select="SELECT id_record, authors AS xml FROM {{source}} WHERE authors IS NOT NULL",
                table=table_authors_raw,
                columns=[
                    ValueColumn("id_record", "INT REFERENCES {{source}}"),
                    ValueColumn("auid", "TEXT"),
                    ValueColumn("seq_no", "INT"),
                    ValueColumn("last_name", "TEXT"),
                    ValueColumn("first_name", "TEXT"),
                    ValueColumn("lang", "TEXT"),
                    ValueColumn("affiliations", "TEXT"),
                ],
                fn=compose(parse_authors, pop_id_fields("id_record")),
            ),
            "dedupe": CreateTableSql(
                table=table_authors,
                sql=folder().joinpath("./nodes/authors_dedupe.sql").read_text(),
                params={"raw": table_authors_raw, "records": table_records},
            ),
        },
        "affiliations": {
            "parse": MapToNewTable(
                source_table=table_authors,
                select="SELECT id_record_author, id_record, affiliations AS xml FROM {{source}} WHERE affiliations IS NOT NULL",
                table=table_rel_aff,
                columns=[
                    ValueColumn("id_record_author", "INT REFERENCES {{source}}"),
                    ValueColumn("id_record", "INT"),
                    ValueColumn("afid", "TEXT"),
                    ValueColumn("addr_no", "INT"),
                    ValueColumn("lang", "TEXT"),
                    ValueColumn("full_address", "TEXT"),
                ],
                fn=compose(
                    parse_affiliations, pop_id_fields("id_record_author", "id_record")
                ),
            ),
            "dedupe": CreateTableSql(
                table=table_affiliations,
                sql=folder().joinpath("./nodes/record_aff.sql").read_text(),
                params={"rel_aff": table_rel_aff, "records": table_records},
            ),
            "connect": AddColumnsSql(
                table=table_rel_aff,
                columns=[
                    Column("id_record_affiliation", "INT REFERENCES {{affiliations}}")
                ],
                sql="""\
                UPDATE {{table}} AS rel
                SET id_record_affiliation = aff.id_record_affiliation
                FROM {{affiliations}} AS aff
                WHERE rel.id_record = aff.id_record AND rel.addr_no = aff.addr_no AND rel.lang = aff.lang;
                """,
                params={"affiliations": table_affiliations},
            ),
        },
    }
