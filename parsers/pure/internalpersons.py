from pathlib import Path
import re
from parsel import Selector
from returns.maybe import Maybe


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


def parse_internalperson(xml: str):
    sel = Selector(xml, type="xml")
    sel.register_namespace(
        "schemaLocation", "http://sciencedata.urfu.ru/ws/api/514/xsd/schema1.xsd"
    )
    sel.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

    for person in sel.xpath("//items/person"):
        date_of_birth = person.xpath("dateOfBirth/text()").get()
        year, education = _find_latest_education(person)

        yield {
            "uuid": person.xpath("@uuid").get(),
            "first_name": person.xpath("name/firstName/text()").get(),
            "last_name": person.xpath("name/lastName/text()").get(),
            "date_of_birth": date_of_birth,
            "year_of_birth": Maybe.from_optional(date_of_birth)
            .bind_optional(lambda date: re.match(r"(\d\d\d\d)-\d\d-\d\d", date))
            .map(lambda match: int(match.group(1)))
            .value_or(None),
            "orcid": person.xpath("orcid/text()").get(),
            "year": year,
            "education": education,
            "qualification": _find_latest_qualification(person),
            "name_variants": person.xpath("nameVariants").extract_first(),
            "ids": person.xpath("ids").extract_first(),
            "associations": person.xpath(
                "staffOrganisationAssociations"
            ).extract_first(),
        }


def parse_internalperson_folder(folder: Path):
    for xml_path in folder.glob("*.xml"):
        filename = xml_path.name

        for person in parse_internalperson(xml_path.read_text()):
            yield {"filename": filename, **person}


def parse_internalperson_variants(xml: str):
    sel = Selector(xml, type="xml")

    for variant in sel.xpath("/nameVariants/nameVariant"):
        yield {
            "firstname": variant.xpath("name/firstName/text()").get(),
            "lastname": variant.xpath("name/lastName/text()").get(),
            "typename": (
                Maybe.from_optional(variant.xpath("type/@uri").get())
                .map(lambda uri: uri.replace("/dk/atira/pure/person/names/", ""))
                .value_or(None)
            ),
        }


def parse_internalperson_ids(xml: str):
    sel = Selector(xml, type="xml")

    for id_node in sel.xpath("/ids/id"):
        yield {
            "id_value": id_node.xpath("value/text()").get(),
            "type_name": (
                Maybe.from_optional(id_node.xpath("type/@uri").get())
                .map(
                    lambda uri: uri.replace("/dk/atira/pure/person/personsources/", "")
                )
                .value_or(None)
            ),
        }


def parse_internalperson_associations(xml: str):
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
            "is_primary_asociation": association.xpath(
                "isPrimaryAssociation/text()"
            ).get()
            == "true",
            "job_description": Maybe.from_optional(
                association.xpath("jobTitle/@uri").get()
            )
            .map(lambda uri: uri.replace("/dk/atira/pure/person/jobtitles/", ""))
            .value_or(None),
            "job_name": association.xpath(
                "jobTitle/term/text[@locale='ru_RU']/text()"
            ).get(),
        }
