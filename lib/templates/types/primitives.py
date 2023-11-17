from typing import Optional
from returns.maybe import Maybe
from sqlglot import exp


def identifier(name: str) -> exp.Identifier:
    return exp.Identifier(this=name, quoted=True)


def table(name: str, schema: Optional[str] = None) -> exp.Table:
    return exp.Table(
        this=identifier(name),
        db=Maybe.from_optional(schema).map(identifier).value_or(None),
    )


def placeholder(name: Optional[str] = None) -> exp.Placeholder:
    return exp.Placeholder(this=name)
