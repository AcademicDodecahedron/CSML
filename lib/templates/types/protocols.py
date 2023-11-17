from typing import Protocol, TypeVar


T = TypeVar("T", covariant=True)


class Renderable(Protocol[T]):
    def render(self, **params) -> T:
        ...


__all__ = ["Renderable"]
