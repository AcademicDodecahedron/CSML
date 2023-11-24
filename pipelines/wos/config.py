from typing import Literal, Optional, Annotated
from pydantic import BaseModel, Field


class SourceParams(BaseModel):
    glob: str = Field(description="Glob for finding source files")


class UssrParams(BaseModel):
    republics: Annotated[list[str], Field(description="List of Soviet republics")] = []


class AddressParams(BaseModel):
    ussr: UssrParams = UssrParams()


class WosSourceParams(SourceParams):
    address: Annotated[
        AddressParams,
        Field(description="Settings for country-specific address parsing"),
    ] = AddressParams()


class WosConfig(BaseModel):
    type: Literal["wos"]
    wos: WosSourceParams = Field(description="Web of Science sources")
    incites: Optional[SourceParams] = Field(description="IcCites sources (optional)")

    def create_tasks(self):
        from .pipeline import create_tasks

        return create_tasks(self)
