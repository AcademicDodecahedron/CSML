from parsel import Selector
from returns.maybe import Maybe, Nothing

from lib.console import console

URI_PREFIX = {
    1: "/dk/atira/pure/organisation/organisationsources/",
    2: "/dk/atira/pure/ueoexternalorganisation/ueoexternalorganisationsources/",
}


def parse_org_ids(xml: str, type_pure_org: int, type_mapping: dict[str, int]):
    sel = Selector(xml, type="xml")

    for item in sel.xpath("/ids/id"):
        id_value = item.xpath("value/text()").get()
        type_name = Maybe.from_optional(item.xpath("type/@uri").get()).map(
            lambda uri: uri.replace(URI_PREFIX[type_pure_org], "")
        )
        type_pure_org_ids = type_name.bind_optional(
            lambda type_name: type_mapping.get(type_name, None)
        )

        if type_pure_org_ids == Nothing:
            console.log(f"Unknown org id type: [red]{type_name.value_or(None)}")

        yield {
            "id_value": id_value,
            "type_name": type_name.value_or(None),
            "type_pure_org_ids": type_pure_org_ids.value_or(None),
        }
