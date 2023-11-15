import re

from pipelines.wos.config import AddressParams


def _noneif(string: str, none_value: str):
    return None if string == none_value else string


def parse_city(city_long: str):
    words = city_long.split()

    city, index = [], []
    for word in words:
        (index if re.search(r"\d", word) else city).append(word)

    return _noneif(" ".join(city), ""), _noneif(" ".join(index), "")


def parse_address_ussr(parts: list[str], config: AddressParams):
    org_name = parts[0]

    if parts[-2] in config.ussr.republics:
        country = " ".join(parts[-2:])
        city, index = parse_city(parts[-3])
    else:
        country = parts[-1]
        city, index = parse_city(parts[-2])

    return {"org_name": org_name, "country": country, "city": city, "index": index}


def parse_address_canada(parts: list[str], config: AddressParams):
    org_name = parts[0]
    country = parts[-1]

    city, index = parse_city(parts[-3])

    return {
        "org_name": org_name,
        "country": country,
        "city": city,
        "index": " ".join([*([index] if index else []), parts[-2]]),
    }


def parse_address(parts: list[str], config: AddressParams):
    org_name = parts[0]
    country = parts[-1]

    city, index = parse_city(parts[-2])

    return {"org_name": org_name, "country": country, "city": city, "index": index}


def split_address(address: str, config: AddressParams):
    parts = re.split(r",\s*", address)

    return {"USSR": parse_address_ussr, "Canada": parse_address_canada}.get(
        parts[-1], parse_address
    )(parts, config)
