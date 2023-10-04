from pathlib import Path
from dataclasses import dataclass
from dataclasses_json import DataClassJsonMixin

from .incites_csv import IncitesCsvReader


@dataclass
class IncitesCsvConfig(DataClassJsonMixin):
    header_length: int
    mapping: dict[str, str]


def parse_record_csv(path: Path, config: IncitesCsvConfig):
    with path.open(encoding="utf-8-sig") as file:
        for row in IncitesCsvReader(file, config.header_length, config.mapping):
            row["sgr"] = row["eid"].replace("2-s2.0-", "")
            yield row


def parse_record_folder(folder: Path, config: IncitesCsvConfig):
    for csv_path in folder.glob("*.csv"):
        filename = csv_path.name

        for row in parse_record_csv(csv_path, config):
            yield {"filename": filename, **row}
