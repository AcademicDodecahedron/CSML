from csv import DictReader
from typing import TextIO
from itertools import islice


def _read_csv_lines(file: TextIO, header_length: int):
    for line in islice(file, header_length, None):
        if line.rstrip() == "":
            break

        yield line


def incited_csv_reader(file: TextIO, header_length: int, *args, **kwargs) -> DictReader:
    return DictReader(_read_csv_lines(file, header_length), *args, **kwargs)
