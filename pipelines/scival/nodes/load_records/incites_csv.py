from csv import DictReader
from typing import Iterable, TextIO
from itertools import islice

from pipelines.scival.config import HeaderLength


def _find_first_csv_line(file: Iterable[str]):
    num_empty = 0

    for line in file:
        if line == "\n":
            num_empty += 1
        else:
            if num_empty >= 2:
                return line
            num_empty = 0

    raise RuntimeError("Couldn't find first line of csv")


def _from_first_csv_line(file: Iterable[str]):
    yield _find_first_csv_line(file)
    yield from file


def _until_empty_line(file: Iterable[str]):
    for line in file:
        if line == "\n":
            break
        yield line


def incited_csv_reader(
    file: TextIO, header_length: HeaderLength, *args, **kwargs
) -> DictReader:
    return DictReader(
        _until_empty_line(
            _from_first_csv_line(file)
            if header_length == "auto"
            else islice(file, header_length, None)
        ),
        *args,
        **kwargs,
    )
