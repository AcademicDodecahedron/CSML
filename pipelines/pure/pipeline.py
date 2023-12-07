from lib import (
    TaskTree,
    table,
    MapToNewTable,
    ValueColumn,
    CreateTableSql,
    AddColumnsSql,
    compose,
    pop_id_fields,
    add_to_input,
)
from lib.utils import folder

from .config import PureConfig
from pipelines.glob import load_files_glob
from .nodes.parse_internalorgs import parse_internalorgs_file
from .nodes.parse_externalorgs import parse_externalorgs_file
from .nodes.parse_org_ids import parse_org_ids
from .nodes.parse_internalpersons import parse_internalpersons_file


def create_tasks(config: PureConfig) -> TaskTree:
    table_internalorg = table("internalorg")
    table_externalorg = table("externalorg")
    table_orgs = table("orgs")
    table_org_ids = table("org_ids")
    table_internalperson = table("internalperson")

    return {
        "orgs": {
            "internalorg": MapToNewTable(
                table=table_internalorg,
                columns=[
                    ValueColumn("filename", "TEXT"),
                    ValueColumn("uuid", "TEXT"),
                    ValueColumn("parent_uuid", "TEXT"),
                    ValueColumn("name_pure_org", "TEXT"),
                    ValueColumn("name_pure_org_eng", "TEXT"),
                    ValueColumn("ids", "TEXT"),
                ],
                fn=load_files_glob(config.internalorg.glob, parse_internalorgs_file),
            ),
            "externalorg": MapToNewTable(
                table=table_externalorg,
                columns=[
                    ValueColumn("filename", "TEXT"),
                    ValueColumn("uuid", "TEXT"),
                    ValueColumn("parent_uuid", "TEXT"),
                    ValueColumn("kind_pure_org", "TEXT"),
                    ValueColumn("name_pure_org", "TEXT"),
                    ValueColumn("name_pure_org_eng", "TEXT"),
                    ValueColumn("country_org", "TEXT"),
                    ValueColumn("city_org", "TEXT"),
                    ValueColumn("ids", "TEXT"),
                ],
                fn=load_files_glob(config.externalorg.glob, parse_externalorgs_file),
            ),
            "join": CreateTableSql(
                table=table_orgs,
                sql=folder().joinpath("./nodes/join_orgs.sql").read_text(),
                params={
                    "internalorg": table_internalorg,
                    "externalorg": table_externalorg,
                },
            ),
            "ids": MapToNewTable(
                source_table=table_orgs,
                select="""\
                    SELECT org_id, ids AS xml, type_pure_org FROM {{source}}
                    WHERE ids IS NOT NULL""",
                table=table_org_ids,
                columns=[
                    ValueColumn("org_id", "INT REFERENCES {{source}}(org_id)"),
                    ValueColumn("id_value", "TEXT"),
                    ValueColumn("type_name", "TEXT"),
                    ValueColumn("type_pure_org_ids", "INT"),
                ],
                fn=compose(
                    parse_org_ids,
                    add_to_input(type_mapping=config.type_pure_org_ids),
                    pop_id_fields("org_id"),
                ),
            ),
            "search_path": AddColumnsSql(
                table=table_orgs,
                sql=folder().joinpath("./nodes/add_search_path.sql").read_text(),
            ),
        },
        "internalperson": MapToNewTable(
            table=table_internalperson,
            columns=[
                ValueColumn("filename", "TEXT"),
                ValueColumn("uuid", "TEXT"),
                ValueColumn("first_name", "TEXT"),
                ValueColumn("last_name", "TEXT"),
                ValueColumn("date_of_birth", "TEXT"),
                ValueColumn("year_or_birth", "INT"),
                ValueColumn("orcid", "TEXT"),
                ValueColumn("year", "INT"),
                ValueColumn("education", "TEXT"),
                ValueColumn("qualification", "TEXT"),
                ValueColumn("name_variants", "TEXT"),
                ValueColumn("ids", "TEXT"),
                ValueColumn("staff_org_ass", "TEXT"),
            ],
            fn=load_files_glob(config.internalperson.glob, parse_internalpersons_file),
        ),
    }
