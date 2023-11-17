from pathlib import Path

from ..config import HeaderLength
from ..incites_csv import incites_csv_reader


def load_records_csv(path: Path, header_length: HeaderLength, mapping: dict[str, str]):
    with path.open(encoding="utf-8-sig") as file:
        for row in incites_csv_reader(file, header_length):
            yield {mapping[key]: value for key, value in row.items() if key in mapping}
