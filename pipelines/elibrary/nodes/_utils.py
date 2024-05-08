from typing import Callable, TypeVar

T = TypeVar("T")
E = TypeVar("E")


def maybe(fn: Callable[[T], E], value: T | None):
    return fn(value) if value else None


def replace(old: str, new: str = ""):
    def inner(s: str):
        return s.replace(old, new)

    return inner
