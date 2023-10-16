from typing import Iterable, Union
from .tasks import Task, TaskSequence


TaskTree = dict[str, Union[Task, "TaskTree"]]


def _flatten_task_tree(tree: TaskTree, prefix: str = "") -> Iterable[tuple[str, Task]]:
    for name, value in tree.items():
        full_name = prefix + name

        if isinstance(value, Task):
            yield full_name, value
        else:
            yield from _flatten_task_tree(value, full_name + ".")


class TaskIndex:
    def __init__(self, definition: TaskTree) -> None:
        self._definition = definition

    def get(self, path: list[str]) -> Task:
        if len(path) == 0:
            raise ValueError("Path cannot be empty")

        next = self._definition
        for i, key in enumerate(path[:-1]):
            next = next[key]
            if not isinstance(next, dict):
                raise RuntimeError(
                    f"Cannot resolve path {path}: {path[:i + 1]} has no children"
                )

        tail = next[path[-1]]
        return (
            tail
            if isinstance(tail, Task)
            else TaskSequence(list(_flatten_task_tree(tail)))
        )

    def full_sequence(self) -> Task:
        return TaskSequence(list(_flatten_task_tree(self._definition)))
