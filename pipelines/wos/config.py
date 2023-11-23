from typing import Literal, Optional
from pydantic import BaseModel, Field


class SourceParams(BaseModel):
    glob: str = Field(description="Glob for finding source files")


class UssrParams(BaseModel):
    republics: list[str] = Field(
        default_factory=list, description="List of Soviet republics"
    )


class AddressParams(BaseModel):
    ussr: UssrParams = Field(default_factory=UssrParams)


class WosSourceParams(SourceParams):
    address: AddressParams = Field(
        default_factory=AddressParams,
        description="Settings for country-specific address parsing",
    )


class WosConfig(BaseModel):
    type: Literal["wos"]
    wos: WosSourceParams = Field(description="Web of Science sources")
    incites: Optional[SourceParams] = Field(description="IcCites sources (optional)")

    def create_tasks(self):
        from .pipeline import create_tasks

        return create_tasks(self)
