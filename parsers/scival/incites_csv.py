from csv import DictReader
from typing import TextIO
from itertools import islice


def _read_csv_lines(file: TextIO, header_length: int):
    for line in islice(file, header_length, None):
        if line.rstrip() == "":
            break

        yield line


class IncitesCsvReader:
    def __init__(
        self, file: TextIO, header_length: int, mapping: dict[str, str] = {}
    ) -> None:
        self._reader = DictReader(_read_csv_lines(file, header_length))
        self._mapping = mapping

    def __iter__(self):
        return self

    def __next__(self):
        return {
            self._mapping[key]: value
            for key, value in next(self._reader).items()
            if key in self._mapping
        }
