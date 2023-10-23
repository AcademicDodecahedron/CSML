from pathlib import Path
from csv import DictReader
import glob

from lib import console


def load_txt(path: Path):
    console.log("Reading", path)
    with path.open(encoding="utf-8-sig") as file:
        yield from DictReader(file, delimiter="\t")


def load_txts_glob(pattern: str):
    for file_path in map(Path, glob.iglob(pattern, recursive=True)):
        filename = file_path.name

        for row in load_txt(file_path):
            yield {"filename": filename, **row}
