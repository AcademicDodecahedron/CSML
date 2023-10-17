from pathlib import Path
from typing import Literal
from pydantic import BaseModel

from lib import TaskTree
from . import pipeline


class ScivalConfig(BaseModel):
    type: Literal["scival"]
    path: Path
    header_length: int
    fields: dict[str, str]
    category_mapping: dict[str, int | str]

    def create_tasks(self) -> TaskTree:
        return pipeline.create_tasks(self)
