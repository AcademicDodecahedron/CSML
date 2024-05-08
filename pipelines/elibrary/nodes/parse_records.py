from parsel import Selector

from ._utils import maybe, replace


def parse_records(xml: str):
    sel = Selector(xml, "xml")

    for item in sel.xpath("/items/item"):
        yield {
            "num_record": item.xpath("@id").get(),
            "year_publ": maybe(int, item.xpath("source/issue/year/text()").get()),
            "cited_from_record": maybe(int, item.xpath("cited/text()").get()),
            "lang_document": item.xpath("language/text()").get(),
            "document_type": item.xpath("type/text()").get(),
            "source_type": item.xpath("genre/text()").get(),
            "source_title": item.xpath("source/journal/title/text()").get(),
            "publisher": item.xpath("source/journal/publisher/text()").get(),
            "source_country": item.xpath("source/journal/country/text()").get(),
            "doi": item.xpath("doi/text()").get(),
            "issn_norm": maybe(
                replace("-"),
                item.xpath("source/journal/issn/text()").get()
                or item.xpath("source/journal/eissn/text()").get(),
            ),
            "refs_count": len(item.xpath("references/reference")),
            "keywords_count": len(item.xpath("keywords/keyword")),
            "authors_count": len(item.xpath("authors/author")),
            "grnti": item.xpath("grnti/text()").get(),
            "vak": item.xpath("source/journal/vak/text()").get(),
            "rsci": item.xpath("source/journal/rsci/text()").get(),
            "wos": item.xpath("source/journal/wos/text()").get(),
            "scopus": item.xpath("source/journal/scopus/text()").get(),
            "authors": item.xpath("authors").get(),
        }


def parse_authors(xml: str):
    authors = Selector(xml, "xml")

    for author in authors.xpath("/authors/author"):
        yield {
            "auid": author.xpath("authorid/text()").get(),
            "seq_no": maybe(int, author.xpath("@num").get()),
            "last_name": author.xpath("lastname/text()").get(),
            "first_name": author.xpath("initials/text()").get(),
            "lang": author.xpath("@lang").get(),
            "affiliations": author.xpath("affiliations").get(),
        }


def parse_affiliations(xml: str):
    affiliations = Selector(xml, "xml")

    for affiliation in affiliations.xpath("/affiliations/affiliation"):
        yield {
            "afid": affiliation.xpath("orgid/text()").get(),
            "addr_no": maybe(int, affiliation.xpath("@num").get()),
            "full_address": affiliation.xpath("orgname/text()").get(),
            "lang": affiliation.xpath("@lang").get(),
        }
