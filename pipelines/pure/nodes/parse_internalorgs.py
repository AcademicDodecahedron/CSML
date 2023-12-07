from pathlib import Path
from parsel import Selector
from returns.maybe import Maybe


def parse_internalorgs_file(path: Path):
    yield from parse_internalorgs_xml(path.read_text())


def parse_internalorgs_xml(xml: str):
    sel = Selector(xml, type="xml")

    for item in sel.xpath("//items/organisationalUnit"):
        yield {
            "uuid": item.xpath("@uuid").get(),
            "parent_uuid": item.xpath("parents/parent/@uuid").get(),
            "kind_pure_org": Maybe.from_optional(item.xpath("type/@uri").get())
            .map(
                lambda uri: uri.replace(
                    "/dk/atira/pure/organisation/organisationtypes/organisation/", ""
                )
            )
            .value_or(None),
            "name_pure_org": item.xpath(
                'nameVariants/nameVariant[@externalId="fullname"]/value/text[@locale = "ru_RU"]/text()'
            ).get(),
            "name_pure_org_eng": item.xpath(
                'nameVariants/nameVariant[@externalId="fullname"]/value/text[@locale = "en_GB"]/text()'
            ).get(),
            "ids": item.xpath("ids").get(),
        }
