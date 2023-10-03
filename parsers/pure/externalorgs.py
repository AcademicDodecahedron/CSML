from returns.maybe import Maybe
from parsel import Selector
from pathlib import Path


def parse_externalorgs(xml: str):
    sel = Selector(xml, type="xml")
    sel.register_namespace(
        "schemaLocation", "http://sciencedata.urfu.ru/ws/api/514/xsd/schema1.xsd"
    )
    sel.register_namespace("xsi", "http://www.w3.org/2001/XMLSchema-instance")

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
            "ids": item.xpath("ids").extract_first(),
        }


def parse_externalorg_folder(folder: Path):
    for xml_path in folder.glob("*.xml"):
        filename = xml_path.name

        for org in parse_externalorgs(xml_path.read_text()):
            yield {"filename": filename, **org}


def parse_externalorg_ids(xml: str):
    sel = Selector(xml, type="xml")

    for id_node in sel.xpath("/ids/id"):
        yield {
            "id_value": id_node.xpath("value/text()").get(),
            "type_name": Maybe.from_optional(id_node.xpath("type/@uri").get())
            .map(
                lambda uri: uri.replace(
                    "/dk/atira/pure/ueoexternalorganisation/ueoexternalorganisationsources/",
                    "",
                )
            )
            .value_or(None),
        }
