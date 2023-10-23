import sqlite3
from argparse import ArgumentParser, Namespace
from typing import Callable, Literal, Optional

from .tasks import Task
from .index import TaskTree, TaskIndex

TaskPath = list[str] | Literal["__full__"]


def _create_action(
    path: TaskPath, action_name: str, db_path: Optional[str], foreign_keys: bool = False
):
    def action(tasks: TaskTree | TaskIndex):
        task_index = TaskIndex(tasks) if isinstance(tasks, dict) else tasks
        task = (
            task_index.full_sequence() if path == "__full__" else task_index.get(path)
        )

        def _with_connection(method: Callable[[sqlite3.Connection], None]):
            if not db_path:
                raise RuntimeError("database path not specified")

            with sqlite3.connect(db_path) as conn:
                if foreign_keys:
                    conn.execute("PRAGMA foreign_keys = 1")

                conn.execute("BEGIN")
                method(conn)

        actions: dict[str, Callable[[Task], None]] = {
            "run": lambda task: _with_connection(task.run),
            "delete": lambda task: _with_connection(task.delete),
            "redo": lambda task: _with_connection(task.redo),
            "describe": lambda task: task.describe(),
        }
        actions[action_name](task)

    return action


def _parse_task_path(string: str) -> TaskPath:
    return "__full__" if string == "__full__" else string.split(".")


class TaskRunnerCli:
    def __init__(self) -> None:
        parser = ArgumentParser()
        parser.add_argument(
            "task", type=_parse_task_path, help="Task path inside the tree"
        )
        parser.add_argument("action", choices=["run", "delete", "redo", "describe"])
        parser.add_argument("-d", "--db", help="sqlite database path")
        parser.add_argument("--fk", action="store_true", help="PRAGMA foreign_keys")
        self._argparser = parser

        custom_group = parser.add_argument_group("custom", "Custom arguments")
        self._custom_group = custom_group

    def add_argument(self, *args, **kwargs):
        if (
            not args
            or len(args) == 1
            and args[0][0] not in self._argparser.prefix_chars
        ):
            raise ValueError("Only keyword arguments are allowed")

        self._custom_group.add_argument(*args, **kwargs)

    def parse_args(self):
        args = self._argparser.parse_args()
        custom_args = {
            action.dest: getattr(args, action.dest)
            for action in self._custom_group._group_actions
        }

        return _create_action(
            args.task, args.action, args.db, foreign_keys=args.fk
        ), Namespace(**custom_args)
