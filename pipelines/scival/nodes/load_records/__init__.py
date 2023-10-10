from pathlib import Path

from .incites_csv import IncitesCsvReader


def load_records_csv(path: Path, header_length: int, mapping: dict[str, str]):
    with path.open(encoding="utf-8-sig") as file:
        for row in IncitesCsvReader(file, header_length, mapping):
            row["sgr"] = row["eid"].replace("2-s2.0-", "")
            yield row


def load_records_csv_or_folder(path: Path, header_length: int, mapping: dict[str, str]):
    for csv_path in path.glob("*.csv") if path.is_dir() else [path]:
        filename = csv_path.name

        for row in load_records_csv_or_folder(csv_path, header_length, mapping):
            yield {"filename": filename, **row}
