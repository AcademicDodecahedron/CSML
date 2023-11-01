import re
from typing import Iterable, Optional
from returns.maybe import Maybe


def _split_c1(c1: str) -> Iterable[tuple[Optional[str], str]]:
    start = 0

    while start < len(c1):
        authors = None

        if c1[start] == "[":
            authors_end = c1.find("]", start)
            authors = c1[start + 1 : authors_end]
            start = authors_end + 2

        block_end = c1.find(";", start)
        if block_end == -1:
            block_end = len(c1)

        yield authors, c1[start:block_end]
        start = block_end + 2


def parse_rel_affiliations(c1: str):
    for order, (authors, address) in enumerate(_split_c1(c1)):
        if authors:
            for author in authors.split("; "):
                yield {
                    "author_name": author,
                    "full_address": address,
                    "order": order + 1,
                }
        else:
            yield {
                "author_name": None,  # This address refers to all authors in the record
                "full_address": address,
                "order": order + 1,
            }


def split_address(address: str):
    parts = re.split(r",\s*", address)

    org_name = parts[0]
    country = parts[-1]
    city_full = parts[-2]

    city = city_full[
        (
            Maybe.from_optional(re.search(r"^((\w+-)?[0-9]+\s+)*", city_full))
            .map(lambda match: match.end(0))
            .value_or(0)
        ) : (
            Maybe.from_optional(re.search(r"(\s+(\w+-)?[0-9]+)*$", city_full))
            .map(lambda match: match.start(0))
            .value_or(len(city_full) - 1)
        )
    ]

    return {"org_name": org_name, "country": country, "city": city}
