from typing import Literal, Optional
from pydantic import BaseModel


class XmlSource(BaseModel):
    type: Literal["xml"]
    glob: str


class PureConfig(BaseModel):
    type: Literal["pure"]
    internalorg: Optional[XmlSource] = None
    externalorg: Optional[XmlSource] = None
    type_pure_org_ids: dict[str, int] = {}
    internalperson: Optional[XmlSource] = None
    records: Optional[XmlSource] = None
    type_pure_person_id: dict[str, int] = {}
    type_pure_person_name: dict[str, int] = {}
    type_record_ids: dict[str, int] = {}
    journals: Optional[XmlSource] = None

    def create_tasks(self):
        from .pipeline import create_tasks

        return create_tasks(self)
