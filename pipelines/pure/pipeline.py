from typing import Literal
from pydantic import BaseModel

from lib import TaskTree


class PureConfig(BaseModel):
    type: Literal["pure"]

    def create_tasks(self) -> TaskTree:
        return create_tasks(self)


def create_tasks(config: PureConfig):
    return {}
