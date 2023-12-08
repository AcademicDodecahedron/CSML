from typing import Literal
from pydantic import BaseModel


class XmlSource(BaseModel):
    type: Literal["xml"]
    glob: str


class PureConfig(BaseModel):
    type: Literal["pure"]
    internalorg: XmlSource
    externalorg: XmlSource
    type_pure_org_ids: dict[str, int] = {}
    internalperson: XmlSource
    records: XmlSource
    type_pure_person_id: dict[str, int] = {}
    type_pure_person_name: dict[str, int] = {}
    type_record_ids: dict[str, int] = {}

    def create_tasks(self):
        from .pipeline import create_tasks

        return create_tasks(self)
