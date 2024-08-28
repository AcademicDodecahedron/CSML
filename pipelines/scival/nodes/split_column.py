import re

_DELIMITER = re.compile(r"\|\s*")


def split_column(column_name: str):
    def split_column_inner(**kwargs):
        column_value = kwargs.pop(column_name)

        for split_value in _DELIMITER.split(column_value):
            yield {**kwargs, column_name: split_value}

    return split_column_inner


def split_categories(category_mapping: dict[str, int]):
    def split_categories_inner(id_record: int, **kwargs):
        for column_name, category in category_mapping.items():
            header = {
                "id_record": id_record,
                "field_name": column_name,
                "type_category": category,
            }
            column_value = kwargs[column_name]

            if column_value == "-":
                continue

            for split_value in _DELIMITER.split(kwargs[column_name]):
                yield {**header, "value_category": split_value}

    return split_categories_inner
