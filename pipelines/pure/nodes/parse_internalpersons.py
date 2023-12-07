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
            "year_or_birth": (
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
            "staff_org_ass": person.xpath(
                "staffOrganisationAssociations/staffOrganisationAssociation"
            ).get(),
        }


def parse_internalpersons_file(path: Path):
    yield from parse_internalpersons_xml(path.read_text())
