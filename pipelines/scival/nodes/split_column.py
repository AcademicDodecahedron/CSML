import re

_DELIMITER = re.compile(r"[,|]\s*")


def _split_column_by_name(row: dict, column_name: str, rename_to: str):
    column_value = row.pop(column_name)

    for split_value in _DELIMITER.split(column_value):
        yield {**row, rename_to: split_value}


def split_column(column_name: str):
    def split_column_inner(**kwargs):
        yield from _split_column_by_name(kwargs, column_name, column_name)

    return split_column_inner


def split_columns_many(*column_names: str):
    def split_columns_inner(**kwargs):
        for column_name in column_names:
            if kwargs[column_name] == "-":
                continue

            for row in _split_column_by_name(kwargs, column_name, "value_category"):
                yield {"field_name": column_name, **row}

    return split_columns_inner
