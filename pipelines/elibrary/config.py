from typing import Literal, Optional
from pydantic import BaseModel

from . import pipeline


class ElibraryConfig(BaseModel):
    type: Literal["elibrary"]
    glob: str
    encoding: Optional[str] = None

    def create_tasks(self):
        return pipeline.create_tasks(self)
