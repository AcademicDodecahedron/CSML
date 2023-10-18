from pathlib import Path
from typing import Literal
from pydantic import BaseModel

from lib import TaskTree
from . import pipeline

HeaderLength = int | Literal["auto"]


class ScivalConfig(BaseModel):
    type: Literal["scival"]
    path: Path
    header_length: HeaderLength
    fields: dict[str, str]
    category_mapping: dict[str, int]

    def create_tasks(self) -> TaskTree:
        return pipeline.create_tasks(self)