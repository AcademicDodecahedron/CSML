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
from .nodes.parse_internalpersons import (
    parse_internalpersons_file,
    parse_person_name_variants,
    parse_person_ids,
    parse_person_associations,
)
from .nodes.parse_records import (
    parse_records_file,
    parse_record_categories,
    parse_record_ids,
    parse_record_authors,
    parse_author_affiliations,
    parse_record_org_units,
    parse_projects,
)


def create_tasks(config: PureConfig) -> TaskTree:
    table_internalorg = table("internalorg")
    table_externalorg = table("externalorg")
    table_orgs = table("orgs")
    table_org_ids = table("org_ids")
    table_internalperson = table("internalperson")
    table_record = table("record")
    table_record_categories = table("record_categories")
    table_record_ids = table("record_ids")
    table_record_authors = table("record_authors")
    table_author_rel_affiliations = table("record_author_rel_affiliations")
    table_record_org_units = table("record_org_units")
    table_record_projects = table("record_projects")
    table_record_affiliations = table("record_affiliations")
    table_projects = table("projects")
    table_person = table("person")
    table_person_name_variants = table("person_name_variants")
    table_person_ids = table("person_ids")
    table_person_associations = table("person_associations")

    return {
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
        "internalperson": MapToNewTable(
            table=table_internalperson,
            columns=[
                ValueColumn("filename", "TEXT"),
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
                ValueColumn("staff_org_ass", "TEXT"),
            ],
            fn=load_files_glob(config.internalperson.glob, parse_internalpersons_file),
        ),
        "record": {
            "load": MapToNewTable(
                table=table_record,
                columns=[
                    "id_record INTEGER PRIMARY KEY AUTOINCREMENT",
                    ValueColumn("filename", "TEXT"),
                    ValueColumn("node_name", "TEXT"),
                    ValueColumn("record_uuid", "TEXT"),
                    ValueColumn("journal_title", "TEXT"),
                    ValueColumn("output_type", "TEXT"),
                    ValueColumn("issn", "TEXT"),
                    ValueColumn("journal_uuid", "TEXT"),
                    ValueColumn("external_id", "TEXT"),
                    ValueColumn("external_id_source", "TEXT"),
                    ValueColumn("doi", "TEXT"),
                    ValueColumn("year_publication", "INT"),
                    ValueColumn("keyword_groups", "TEXT"),
                    ValueColumn("additional_external_ids", "TEXT"),
                    ValueColumn("person_associations", "TEXT"),
                    ValueColumn("organisational_units", "TEXT"),
                    ValueColumn("related_projects", "TEXT"),
                ],
                fn=load_files_glob(config.records.glob, parse_records_file),
            ),
            "categories": MapToNewTable(
                source_table=table_record,
                select="""\
                    SELECT id_record, keyword_groups AS xml FROM {{source}}
                    WHERE keyword_groups IS NOT NULL""",
                table=table_record_categories,
                columns=[
                    ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                    ValueColumn("type_category", "INT"),
                    ValueColumn("value_category", "TEXT"),
                ],
                fn=compose(parse_record_categories, pop_id_fields("id_record")),
            ),
            "ids": MapToNewTable(
                source_table=table_record,
                select="SELECT id_record, external_id_source, external_id, additional_external_ids FROM {{source}}",
                table=table_record_ids,
                columns=[
                    ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                    ValueColumn("source_name", "TEXT"),
                    ValueColumn("id_value", "TEXT"),
                    ValueColumn("id_source", "INT"),
                ],
                fn=compose(
                    parse_record_ids,
                    add_to_input(type_record_ids=config.type_record_ids),
                    pop_id_fields("id_record"),
                ),
            ),
            "authors": {
                "load": MapToNewTable(
                    source_table=table_record,
                    select="SELECT id_record, person_associations AS xml FROM {{source}} WHERE person_associations IS NOT NULL",
                    table=table_record_authors,
                    columns=[
                        "id_record_author INTEGER PRIMARY KEY AUTOINCREMENT",
                        ValueColumn(
                            "id_record", "INT REFERENCES {{source}}(id_record)"
                        ),
                        ValueColumn("full_name", "TEXT"),
                        ValueColumn("first_name", "TEXT"),
                        ValueColumn("last_name", "TEXT"),
                        ValueColumn("role", "TEXT"),
                        ValueColumn("person_uuid", "TEXT"),
                        ValueColumn("external_person_uuid", "TEXT"),
                        ValueColumn("organisational_units", "TEXT"),
                        ValueColumn("external_organisations", "TEXT"),
                    ],
                    fn=compose(parse_record_authors, pop_id_fields("id_record")),
                ),
                "affiliations": MapToNewTable(
                    source_table=table_record_authors,
                    select="SELECT id_record_author, id_record, organisational_units, external_organisations FROM {{source}}",
                    table=table_author_rel_affiliations,
                    columns=[
                        ValueColumn(
                            "id_record_author",
                            "INT REFERENCES {{source}}(id_record_author)",
                        ),
                        ValueColumn(
                            "id_record", "INT REFERENCES {{records}}(id_record)"
                        ),
                        ValueColumn("org_uuid", "TEXT"),
                        ValueColumn("full_address", "TEXT"),
                        ValueColumn("only_record_level", "INT"),
                    ],
                    fn=compose(
                        parse_author_affiliations,
                        pop_id_fields("id_record_author", "id_record"),
                    ),
                    params={"records": table_record},
                ),
            },
            "org_units": MapToNewTable(
                source_table=table_record,
                select="SELECT id_record, organisational_units AS xml FROM {{source}} WHERE organisational_units IS NOT NULL",
                table=table_record_org_units,
                columns=[
                    ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                    ValueColumn("org_uuid", "TEXT"),
                    ValueColumn("full_address", "TEXT"),
                    ValueColumn("only_record_level", "TEXT"),
                ],
                fn=compose(parse_record_org_units, pop_id_fields("id_record")),
            ),
            "record_projects": MapToNewTable(
                source_table=table_record,
                select="SELECT id_record, related_projects AS xml FROM {{source}} WHERE related_projects IS NOT NULL",
                table=table_record_projects,
                columns=[
                    ValueColumn("id_record", "INT REFERENCES {{source}}(id_record)"),
                    ValueColumn("uuid", "TEXT"),
                    ValueColumn("type_classification", "TEXT"),
                    ValueColumn("title", "TEXT"),
                ],
                fn=compose(parse_projects, pop_id_fields("id_record")),
            ),
            "join_affiliations": CreateTableSql(
                table=table_record_affiliations,
                sql=folder().joinpath("./nodes/record_affiliations.sql").read_text(),
                params={
                    "records": table_record,
                    "author_affiliations": table_author_rel_affiliations,
                    "org_units": table_record_org_units,
                },
            ),
            "map_rel_affiliations": AddColumnsSql(
                table=table_author_rel_affiliations,
                sql=folder()
                .joinpath("./nodes/author_rel_affiliations.sql")
                .read_text(),
                params={
                    "affiliations": table_record_affiliations,
                },
            ),
            "projects_dedupe": CreateTableSql(
                table=table_projects,
                sql=folder().joinpath("./nodes/projects_dedupe.sql").read_text(),
                params={"record_projects": table_record_projects},
            ),
            "map_projects": AddColumnsSql(
                table=table_record_projects,
                sql=folder().joinpath("./nodes/map_projects.sql").read_text(),
                params={"projects": table_projects},
            ),
        },
        "orgs": {
            "join": CreateTableSql(
                table=table_orgs,
                sql=folder().joinpath("./nodes/join_orgs.sql").read_text(),
                params={
                    "internalorg": table_internalorg,
                    "externalorg": table_externalorg,
                    "record_affiliations": table_record_affiliations,
                    "records": table_record,
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
        "persons": {
            "join": CreateTableSql(
                table=table_person,
                sql=folder().joinpath("./nodes/join_persons.sql").read_text(),
                params={
                    "internalperson": table_internalperson,
                    "records": table_record,
                    "authors": table_record_authors,
                },
            ),
            "name_variants": MapToNewTable(
                source_table=table_person,
                select="SELECT id_person, name_variants AS xml FROM {{source}} WHERE name_variants IS NOT NULL",
                table=table_person_name_variants,
                columns=[
                    ValueColumn("id_person", "INT REFERENCES {{source}}(id_person)"),
                    ValueColumn("firstname", "TEXT"),
                    ValueColumn("lastname", "TEXT"),
                    ValueColumn("typename", "TEXT"),
                    ValueColumn("type_id", "INT"),
                ],
                fn=compose(
                    parse_person_name_variants,
                    add_to_input(type_pure_person_name=config.type_pure_person_name),
                    pop_id_fields("id_person"),
                ),
            ),
            "ids": MapToNewTable(
                source_table=table_person,
                select="SELECT id_person, ids AS xml FROM {{source}} WHERE ids IS NOT NULL",
                table=table_person_ids,
                columns=[
                    ValueColumn("id_person", "INT REFERENCES {{source}}(id_person)"),
                    ValueColumn("id_value", "TEXT"),
                    ValueColumn("type_name", "TEXT"),
                    ValueColumn("type_id", "INT"),
                ],
                fn=compose(
                    parse_person_ids,
                    add_to_input(type_pure_person_id=config.type_pure_person_id),
                    pop_id_fields("id_person"),
                ),
            ),
            "associations": {
                "parse": MapToNewTable(
                    source_table=table_person,
                    select="SELECT id_person, staff_org_ass AS xml FROM {{source}} WHERE staff_org_ass IS NOT NULL",
                    table=table_person_associations,
                    columns=[
                        ValueColumn(
                            "id_person", "INT REFERENCES {{source}}(id_person)"
                        ),
                        ValueColumn("uuid_org", "TEXT"),
                        ValueColumn("type_work", "TEXT"),
                        ValueColumn("start_date", "TEXT"),
                        ValueColumn("end_date", "TEXT"),
                        ValueColumn("is_primary_asociation", "INT"),
                        ValueColumn("job_description", "TEXT"),
                        ValueColumn("job_name", "TEXT"),
                    ],
                    fn=compose(parse_person_associations, pop_id_fields("id_person")),
                ),
                "map": AddColumnsSql(
                    table=table_person_associations,
                    sql=folder()
                    .joinpath("./nodes/map_person_associations.sql")
                    .read_text(),
                    params={"orgs": table_orgs},
                ),
            },
        },
        "map_affiliations": AddColumnsSql(
            table=table_record_affiliations,
            sql=folder().joinpath("./nodes/map_affiliations.sql").read_text(),
            params={"orgs": table_orgs},
        ),
        "map_authors": AddColumnsSql(
            table=table_record_authors,
            sql=folder().joinpath("./nodes/map_record_authors.sql").read_text(),
            params={"person": table_person},
        ),
    }
