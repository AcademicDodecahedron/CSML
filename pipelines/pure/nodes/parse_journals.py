from pathlib import Path
from parsel import Selector
from returns.maybe import Maybe


def parse_journals_xml(xml: str):
    sel = Selector(xml, type="xml")

    for journal in sel.xpath("/result/items/journal"):
        yield {
            "uuid": journal.xpath("@uuid").get(),
            "source_title": journal.xpath("titles/title/text()").get(),
            "source_type": (
                Maybe.from_optional(journal.xpath("type/@uri").get())
                .map(lambda uri: uri.replace("/dk/atira/pure/", ""))
                .value_or(None)
            ),
            "issn_norm": (
                journal.xpath("issns/issn/text()").get()
                or journal.xpath("electronicISSNs/electronicISSN/text()").get()
            ),
            "publisher": journal.xpath("publisher/name/text/text()").get(),
            "country": (
                Maybe.from_optional(journal.xpath("country/@uri").get())
                .map(lambda uri: uri.replace("/dk/atira/pure/core/countries/", ""))
                .value_or(None)
            ),
        }


def parse_journals_file(path: Path):
    yield from parse_journals_xml(path.read_text())
