from pathlib import Path
from typing import Any, Literal
from pydantic import BaseModel, model_validator


class TypeCategory(BaseModel):
    type: int | str
    split: bool = True

    @model_validator(mode="before")
    @classmethod
    def allow_dict_or_value(cls, data: Any) -> dict:
        if not isinstance(data, dict):
            return {"type": data}
        else:
            return data


class ScivalConfig(BaseModel):
    type: Literal["scival"]
    path: Path
    header_length: int
    fields: dict[str, str]
    category_mapping: dict[str, TypeCategory]
