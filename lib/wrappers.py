from typing import Callable


def with_args(fn, **add_kwargs):
    def wrapper(**kwargs):
        yield from fn(**kwargs, **add_kwargs)

    return wrapper


def with_args_one(fn, **add_kwargs):
    def wrapper(**kwargs):
        return fn(**kwargs, **add_kwargs)

    return wrapper


def rename_output(fn, mapping: dict[str, str] | Callable[[str], str]):
    remap = (lambda key: mapping[key]) if isinstance(mapping, dict) else mapping

    def wrapper(**kwargs):
        for row in fn(**kwargs):
            yield {remap(key): value for key, value in row.items()}

    return wrapper


def pop_id_fields(fn, *id_fields: str):
    def wrapper(**kwargs):
        id_values = {name: kwargs.pop(name) for name in id_fields}
        for row in fn(**kwargs):
            yield {**id_values, **row}

    return wrapper


def pop_id_fields_one(fn, *id_fields: str):
    def wrapper(**kwargs):
        id_values = {name: kwargs.pop(name) for name in id_fields}
        return {**id_values, **fn(**kwargs)}

    return wrapper
