import re
import csv
from pathlib import Path
from typing import TextIO


def incites_reader(file: TextIO) -> tuple[csv.DictReader, dict[str, str]]:
    lines = file.readlines()

    csv_end = lines.index("\n")
    footer_start = csv_end + 1
    footer_end = lines.index("\n", footer_start)

    footer_dict = {}
    for footer_row in csv.reader(lines[footer_start:footer_end]):
        key, value = re.split(r":\s*", footer_row[0], 1)
        footer_dict[key] = value

    return csv.DictReader(lines[:csv_end]), footer_dict


def load_incites(path: Path):
    with path.open(encoding="utf-8-sig") as file:
        main_reader, footer = incites_reader(file)

        for row in main_reader:
            yield {**row, **{"footer__" + key: value for key, value in footer.items()}}
