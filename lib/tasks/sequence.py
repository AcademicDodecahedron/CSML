from sqlite3 import Connection
from .base import Task


class TaskSequence(Task):
    def __init__(self, definition: list[tuple[str, Task]]) -> None:
        super().__init__()
        self._definition = definition

    def run(self, conn: Connection):
        for name, task in self._definition:
            print("Running", name)

            task.run(conn)
            conn.commit()

    def delete(self, conn: Connection):
        for name, task in reversed(self._definition):
            print("Deleting", name)

            task.delete(conn)
            conn.commit()

    def describe(self):
        for name, _ in self._definition:
            print(name)
