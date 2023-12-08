import re
from pathlib import Path
from typing import Optional
from parsel import Selector
from returns.maybe import Maybe

from lib.console import console


def parse_records_xml(xml: str):
    sel = Selector(xml, type="xml")

    for record in sel.xpath("//items/*"):
        yield {
            "node_name": record.xpath("name()").get(),
            "record_uuid": record.xpath("@uuid").get(),
            "journal_title": record.xpath("journalAssociation/title/text()").get(),
            "output_type": (
                Maybe.from_optional(record.xpath("type/@uri").get())
                .map(
                    lambda uri: uri.replace(
                        "/dk/atira/pure/researchoutput/researchoutputtypes/", ""
                    )
                )
                .value_or(None)
            ),
            "issn": (
                Maybe.from_optional(
                    record.xpath("journalAssociation/issn/text()").get()
                )
                .map(lambda issn: issn.replace("-", ""))
                .value_or(None)
            ),
            "journal_uuid": record.xpath("journalAssociation/journal/@uuid").get(),
            "external_id": record.xpath("@externalId").get(),
            "external_id_source": record.xpath("@externalIdSource").get(),
            "doi": (
                Maybe.from_optional(record.xpath(".//doi/text()").get())
                .map(lambda doi: re.sub("^https://doi.org/", "", doi))
                .value_or(None)
            ),
            "year_publication": Maybe.from_optional(
                record.xpath(
                    "publicationStatuses/publicationStatus/\
                publicationStatus[@uri='/dk/atira/pure/researchoutput/status/published']/../\
                publicationDate/year/text()"
                ).get()
            )
            .map(int)
            .value_or(None),
            "keyword_groups": record.xpath("keywordGroups").get(),
            "additional_external_ids": record.xpath("info/additionalExternalIds").get(),
            "person_associations": record.xpath("personAssociations").get(),
            "organisational_units": record.xpath("organisationalUnits").get(),
            "related_projects": record.xpath("relatedProjects").get(),
        }


def parse_records_file(path: Path):
    yield from parse_records_xml(path.read_text())


def parse_record_categories(xml: str):
    sel = Selector(xml, type="xml")

    for keyword in sel.xpath(
        "/keywordGroups/keywordGroup/keywordContainers/keywordContainer/structuredKeyword"
    ):
        if uri := keyword.xpath("@uri").get():
            if uri == "/ru/urfu/quality_research_output/VAK":
                yield {"type_category": 8, "value_category": "VAK"}
            elif asjc_match := re.match(
                r"/dk/atira/pure/subjectarea/asjc/\d+/(\d+)", uri
            ):
                yield {"type_category": 5, "value_category": asjc_match.group(1)}


def parse_record_ids(
    external_id_source: Optional[str],
    external_id: Optional[str],
    additional_external_ids: Optional[str],
    type_record_ids: dict[str, int],
):
    def get_id_source(source_name: Optional[str]):
        id_source = (
            Maybe.from_optional(source_name)
            .bind_optional(lambda source_name: type_record_ids.get(source_name, None))
            .value_or(None)
        )

        if not id_source:
            console.log(f"Unknown record id type: [red]{source_name}")

        return id_source

    if external_id_source:
        yield {
            "source_name": external_id_source,
            "id_value": external_id,
            "id_source": get_id_source(external_id_source),
        }

    if additional_external_ids:
        sel = Selector(additional_external_ids, type="xml")

        for id_node in sel.xpath("/additionalExternalIds/id"):
            source_name = id_node.xpath("@idSource").get()

            yield {
                "source_name": source_name,
                "id_value": id_node.xpath("text()").get(),
                "id_source": get_id_source(source_name),
            }


def parse_record_authors(xml: str):
    sel = Selector(xml, type="xml")

    for person_association in sel.xpath("/personAssociations/personAssociation"):
        first_name = person_association.xpath("name/firstName/text()").get()
        last_name = person_association.xpath("name/lastName/text()").get()
        role = (
            Maybe.from_optional(person_association.xpath("personRole/@uri").get())
            .map(lambda uri: uri.replace("/dk/atira/pure/researchoutput/roles/", ""))
            .value_or(None)
        )

        external_person_uuid = person_association.xpath("externalPerson/@uuid").get()
        person_uuid = person_association.xpath("person/@uuid").get()

        if not (person_uuid or external_person_uuid):
            continue

        yield {
            "full_name": f"{first_name} {last_name}",
            "first_name": first_name,
            "last_name": last_name,
            "role": role,
            "person_uuid": person_uuid,
            "external_person_uuid": external_person_uuid,
            "organisational_units": person_association.xpath(
                "organisationalUnits"
            ).get(),
            "external_organisations": person_association.xpath(
                "externalOrganisations"
            ).get(),
        }


def parse_author_affiliations(
    organisational_units: Optional[str], external_organisations: Optional[str]
):
    if organisational_units:
        for org_unit in Selector(organisational_units, type="xml").xpath(
            "/organisationalUnits/organisationalUnit"
        ):
            yield {
                "org_uuid": org_unit.xpath("@uuid").get(),
                "full_address": "Ural Federal Univ",
                "only_record_level": 0,
            }

    if external_organisations:
        for ext_org in Selector(external_organisations, type="xml").xpath(
            "/externalOrganisations/externalOrganisation"
        ):
            yield {
                "org_uuid": ext_org.xpath("@uuid").get(),
                "full_address": ext_org.xpath(
                    "name/text[@locale='en_GB']/text()"
                ).get(),
                "only_record_level": 0,
            }


def parse_record_org_units(xml: str):
    sel = Selector(xml, type="xml")

    for org_association in sel.xpath("/organisationalUnits/organisationalUnit"):
        yield {
            "org_uuid": org_association.xpath("@uuid").get(),
            "full_address": "SubUrFU",
            "only_record_level": 1,
        }


def parse_projects(xml: str):
    sel = Selector(xml, type="xml")

    for related_project in sel.xpath("/relatedProjects/relatedProject"):
        uuid_project = related_project.xpath("@uuid").get()
        type_classification = related_project.xpath(
            "type/term/text[@locale='en_GB']/text()"
        ).get()
        title = (
            Maybe.from_optional(related_project.xpath("type/@uri").get())
            .map(
                lambda uri: uri.replace(
                    "/dk/atira/pure/upmproject/upmprojecttypes/upmproject/", ""
                )
            )
            .value_or(None)
        )

        yield {
            "uuid": uuid_project,
            "type_classification": type_classification,
            "title": title,
        }
