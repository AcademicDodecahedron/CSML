from csv import DictReader
from pathlib import Path


def load_wos(path: Path):
    with path.open(encoding="utf-8-sig") as file:
        yield from DictReader(file, delimiter="\t")
