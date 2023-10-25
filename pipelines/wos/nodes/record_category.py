def split_category(category_mapping: dict[str, int]):
    def split_categories_inner(id_record: int, **kwargs):
        for column_name, category in category_mapping.items():
            header = {
                "id_record": id_record,
                "field_name": column_name,
                "type_category": category,
            }
            column_value = kwargs[column_name]

            if column_value:
                for split_value in column_value.split("; "):
                    yield {**header, "value_category": split_value}

    return split_categories_inner
