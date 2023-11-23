from typing import Literal
from pydantic import BaseModel, Field

from . import pipeline

HeaderLength = int | Literal["auto"]


class ScivalConfig(BaseModel):
    type: Literal["scival"]
    glob: str = Field(
        description="A *glob* pattern for finding CSVs, such as ``./folder/*.csv``"
    )
    header_length: HeaderLength = Field(
        description="number of lines to skip, or **auto** for auto-detection"
    )
    fields: dict[str, str] = Field(
        description="""\
Fields that will be imported into the database for further processing
\n
The key is the CSV field name, and the value is the column name in *sqlite* table"""
    )
    category_mapping: dict[str, int] = Field(
        description="Mapping a column name from **fields** to the corresponding ``csml_record_category.type_category`` id"
    )

    def create_tasks(self):
        return pipeline.create_tasks(self)
