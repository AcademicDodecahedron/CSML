from pathlib import Path
from parsel import Selector
from returns.maybe import Maybe


def parse_externalorgs_file(path: Path):
    yield from parse_externalorgs_xml(path.read_text())


def parse_externalorgs_xml(xml: str):
    sel = Selector(xml, type="xml")

    for item in sel.xpath("/result/items/externalOrganisation"):
        yield {
            "uuid": item.xpath("@uuid").get(),
            "parent_uuid": item.xpath("parent/@uuid").get(),
            "kind_pure_org": Maybe.from_optional(item.xpath("type/@uri").get())
            .map(
                lambda uri: uri.replace(
                    "/dk/atira/pure/ueoexternalorganisation/ueoexternalorganisationtypes/ueoexternalorganisation/",
                    "",
                )
            )
            .value_or(None),
            "name_pure_org": item.xpath("name/text[@locale='ru_RU']/text()").get(),
            "name_pure_org_eng": item.xpath("name/text[@locale='en_GB']/text()").get(),
            "country_org": Maybe.from_optional(item.xpath("address/country/@uri").get())
            .map(lambda uri: uri.replace("/dk/atira/pure/core/countries/", ""))
            .value_or(None),
            "city_org": item.xpath("address/city/text()").get(),
            "ids": item.xpath("ids").get(),
        }
