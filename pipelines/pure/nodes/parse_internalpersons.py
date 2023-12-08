from pathlib import Path
import re
from parsel import Selector
from returns.maybe import Maybe, Nothing

from lib.console import console


def _find_latest_education(person: Selector):
    latest_year = None
    education_name = None

    for education in person.xpath("educations/education"):
        year = (
            Maybe.from_optional(education.xpath("period/startDate/year/text()").get())
            .map(int)
            .value_or(None)
        )
        if year and (latest_year is None or latest_year < year):
            latest_year = year
            education_name = (
                Maybe.from_optional(education.xpath("qualification/@uri").get())
                .map(
                    lambda uri: uri.replace(
                        "/dk/atira/pure/personeducation/qualification/", ""
                    )
                )
                .value_or(None)
            )

    return latest_year, education_name


def _find_latest_qualification(person: Selector):
    latest_year = None
    qualification_name = None

    for qualification in person.xpath(
        "professionalQualifications/professionalQualification"
    ):
        year = (
            Maybe.from_optional(
                qualification.xpath("period/startDate/year/text()").get()
            )
            .map(int)
            .value_or(None)
        )
        if year and (latest_year is None or latest_year < year):
            latest_year = year
            qualification_name = qualification.xpath(
                "abbreviatedQualification/text[@locale='ru_RU']/text()"
            ).get()

    return qualification_name


def parse_internalpersons_xml(xml: str):
    sel = Selector(xml, type="xml")

    for person in sel.xpath("//items/person"):
        date_of_birth = person.xpath("dateOfBirth/text()").get()
        year, education = _find_latest_education(person)

        yield {
            "uuid": person.xpath("@uuid").get(),
            "first_name": person.xpath("name/firstName/text()").get(),
            "last_name": person.xpath("name/lastName/text()").get(),
            "date_of_birth": date_of_birth,
            "year_of_birth": (
                Maybe.from_optional(date_of_birth)
                .bind_optional(lambda date: re.match(r"(\d\d\d\d)-\d\d-\d\d", date))
                .map(lambda match: int(match.group(1)))
                .value_or(None)
            ),
            "orcid": person.xpath("orcid/text()").get(),
            "year": year,
            "education": education,
            "qualification": _find_latest_qualification(person),
            "name_variants": person.xpath("nameVariants").get(),
            "ids": person.xpath("ids").get(),
            "staff_org_ass": person.xpath("staffOrganisationAssociations").get(),
        }


def parse_internalpersons_file(path: Path):
    yield from parse_internalpersons_xml(path.read_text())


def parse_person_name_variants(xml: str, type_pure_person_name: dict[str, int]):
    sel = Selector(xml, type="xml")

    for variant in sel.xpath("/nameVariants/nameVariant"):
        type_name = Maybe.from_optional(variant.xpath("type/@uri").get()).map(
            lambda uri: uri.replace("/dk/atira/pure/person/names/", "")
        )
        type_id = type_name.bind_optional(
            lambda type_name: type_pure_person_name.get(type_name, None)
        )

        if type_id == Nothing:
            console.log(f"Unknown person name type: [red]{type_name.value_or(None)}")

        yield {
            "firstname": variant.xpath("name/firstName/text()").get(),
            "lastname": variant.xpath("name/lastName/text()").get(),
            "typename": type_name.value_or(None),
            "type_id": type_id.value_or(None),
        }


def parse_person_ids(xml: str, type_pure_person_id: dict[str, int]):
    sel = Selector(xml, type="xml")

    for id_node in sel.xpath("/ids/id"):
        type_name = Maybe.from_optional(id_node.xpath("type/@uri").get()).map(
            lambda uri: uri.replace("/dk/atira/pure/person/personsources/", "")
        )
        type_id = type_name.bind_optional(
            lambda type_name: type_pure_person_id.get(type_name, None)
        )

        if type_id == Nothing:
            console.log(f"Unknown person id type: [red]{type_name.value_or(None)}")

        yield {
            "id_value": id_node.xpath("value/text()").get(),
            "type_name": type_name.value_or(None),
            "type_id": type_id.value_or(None),
        }


def parse_person_associations(xml: str):
    sel = Selector(xml, type="xml")

    for association in sel.xpath(
        "/staffOrganisationAssociations/staffOrganisationAssociation"
    ):
        yield {
            "uuid_org": association.xpath("organisationalUnit/@uuid").get(),
            "type_work": Maybe.from_optional(
                association.xpath("./employmentType/@uri").get()
            )
            .map(lambda uri: uri.replace("/dk/atira/pure/person/employmenttypes/", ""))
            .value_or(None),
            "start_date": association.xpath("period/startDate/text()").get(),
            "end_date": association.xpath("period/endDate/text()").get(),
            "is_primary_asociation": True
            if association.xpath("isPrimaryAssociation/text()").get() == "true"
            else False,
            "job_description": Maybe.from_optional(
                association.xpath("jobTitle/@uri").get()
            )
            .map(lambda uri: uri.replace("/dk/atira/pure/person/jobtitles/", ""))
            .value_or(None),
            "job_name": association.xpath(
                "jobTitle/term/text[@locale='ru_RU']/text()"
            ).get(),
        }
