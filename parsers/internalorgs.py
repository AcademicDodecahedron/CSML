from returns.maybe import Maybe
from parsel import Selector
from pathlib import Path


def parse_internalorgs(xml: str):
    sel = Selector(xml, type="xml")
    sel.register_namespace(
        "schemaLocation", "http://sciencedata.urfu.ru/ws/api/514/xsd/schema1.xsd"
    )
    sel.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

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
            "ids": item.xpath("ids").extract_first(),
        }


def parse_internalorg_folder(folder: Path):
    for xml_path in folder.glob("*.xml"):
        filename = xml_path.name

        for org in parse_internalorgs(xml_path.read_text()):
            yield {"filename": filename, **org}
