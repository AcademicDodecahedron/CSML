from abc import ABC, abstractmethod
from sqlite3 import Connection
from rich.syntax import Syntax

from lib.console import console


class Task(ABC):
    def __init__(self) -> None:
        self.scripts = {}

    @abstractmethod
    def run(self, conn: Connection):
        pass

    @abstractmethod
    def delete(self, conn: Connection):
        pass

    @abstractmethod
    def exists(self, conn: Connection):
        pass

    def redo(self, conn: Connection):
        self.delete(conn)
        self.run(conn)

    def describe(self):
        for i, (name, script) in enumerate(self.scripts.items()):
            console.print(f"[bold]{name}:")
            console.print(Syntax(script, "sql"))

            if i < len(self.scripts) - 1:
                console.print()
