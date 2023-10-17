from pathlib import Path
from pydantic import BaseModel, Field

from .scival import ScivalConfig
from .pure import PureConfig


SourceConfig = ScivalConfig | PureConfig


class CsmlConfig(BaseModel):
    source: SourceConfig
    sql_schema: list[Path] = Field(alias="schema")
    export: list[Path]
