from sqlite3 import Connection
from rich.progress import track

from .base import Task
from lib.console import console


class TaskSequence(Task):
    def __init__(self, definition: list[tuple[str, Task]]) -> None:
        super().__init__()
        self._definition = definition

    def run(self, conn: Connection):
        for name, task in track(
            self._definition, description="Running tasks...", console=console
        ):
            if task.exists(conn):
                console.print(f"Skipping [bold green]{name}[/bold green]: already done")
            else:
                console.print(f"Running [bold green]{name}")

                task.run(conn)
                conn.commit()

    def delete(self, conn: Connection):
        for name, task in track(
            reversed(self._definition), description="Undoing tasks...", console=console
        ):
            if not task.exists(conn):
                console.print(
                    f"Skipping [bold green]{name}[/bold green]: does not exist"
                )
            else:
                console.print(f"Deleting [bold green]{name}")

                task.delete(conn)
                conn.commit()

    def exists(self, conn: Connection):
        return False

    def describe(self):
        for name, _ in self._definition:
            console.print(name)
