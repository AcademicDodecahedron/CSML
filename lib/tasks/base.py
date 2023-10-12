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

    def redo(self, conn: Connection):
        self.delete(conn)
        self.run(conn)

    def describe(self):
        for name, script in self.scripts.items():
            print(name)
            print(script)
            print()
