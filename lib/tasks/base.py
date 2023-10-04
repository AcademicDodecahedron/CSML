from abc import ABC, abstractmethod
from sqlite3 import Connection


class Task(ABC):
    def __init__(self) -> None:
        self.scripts = {}

    @abstractmethod
    def run(self, conn: Connection):
        pass

    @abstractmethod
    def delete(self, conn: Connection):
        pass
