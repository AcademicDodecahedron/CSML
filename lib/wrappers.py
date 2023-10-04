def with_args(fn, **add_kwargs):
    def wrapper(**kwargs):
        yield from fn(**kwargs, **add_kwargs)

    return wrapper


def pop_id_fields(fn, *id_fields: str):
    def wrapper(**kwargs):
        id_values = {name: kwargs.pop(name) for name in id_fields}
        for row in fn(**kwargs):
            yield {**id_values, **row}

    return wrapper
