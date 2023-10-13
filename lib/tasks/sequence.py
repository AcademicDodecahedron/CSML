from sqlite3 import Connection
from .base import Task


class TaskSequence(Task):
    def __init__(self, definition: list[tuple[str, Task]]) -> None:
        super().__init__()
        self._definition = definition

    def run(self, conn: Connection):
        for name, task in self._definition:
            if task.exists(conn):
                print(f"Skipping {name}: already done")
            else:
                print("Running", name)

                task.run(conn)
                conn.commit()

    def delete(self, conn: Connection):
        for name, task in reversed(self._definition):
            if not task.exists(conn):
                print(f"Skipping {name}: does not exist")
            else:
                print("Deleting", name)

                task.delete(conn)
                conn.commit()

    def exists(self, conn: Connection):
        return False

    def describe(self):
        for name, _ in self._definition:
            print(name)
