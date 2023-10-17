from typing import Literal
from pydantic import BaseModel


class PureConfig(BaseModel):
    type: Literal["pure"]


def create_tasks(config: PureConfig):
    return {}
