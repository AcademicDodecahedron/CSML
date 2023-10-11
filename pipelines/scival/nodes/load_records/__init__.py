from pathlib import Path

from .incites_csv import incited_csv_reader


def load_records_csv(path: Path, header_length: int, mapping: dict[str, str]):
    with path.open(encoding="utf-8-sig") as file:
        for row in incited_csv_reader(file, header_length):
            yield {mapping[key]: value for key, value in row.items() if key in mapping}


def load_records_csv_or_folder(path: Path, header_length: int, mapping: dict[str, str]):
    for csv_path in path.glob("*.csv") if path.is_dir() else [path]:
        filename = csv_path.name

        for row in load_records_csv(csv_path, header_length, mapping):
            yield {"filename": filename, **row}
