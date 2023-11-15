from typing import Literal, Optional
from pydantic import BaseModel, Field


class SourceParams(BaseModel):
    glob: str


class UssrParams(BaseModel):
    republics: list[str] = Field(default_factory=list)


class AddressParams(BaseModel):
    ussr: UssrParams = Field(default_factory=UssrParams)


class WosConfig(BaseModel):
    type: Literal["wos"]
    wos: SourceParams
    incites: Optional[SourceParams]
    address: AddressParams = Field(default_factory=AddressParams)

    def create_tasks(self):
        from .pipeline import create_tasks

        return create_tasks(self)
