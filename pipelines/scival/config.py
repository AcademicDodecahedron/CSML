from pathlib import Path
from typing import Literal
from pydantic import BaseModel


class ScivalConfig(BaseModel):
    type: Literal["scival"]
    path: Path
    header_length: int
    fields: dict[str, str]
    category_mapping: dict[str, int | str]
