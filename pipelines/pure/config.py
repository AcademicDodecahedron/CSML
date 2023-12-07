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

    def create_tasks(self):
        from .pipeline import create_tasks

        return create_tasks(self)
