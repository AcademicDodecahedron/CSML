import sqlite3
import yaml
from pathlib import Path
from pydantic import BaseModel, Field
from argparse import ArgumentParser
from rich.progress import track

from lib import TaskTree, TaskIndex, sql_environment, sql_adapter, console
from pipelines import scival, pure, SourceConfig


class CsmlConfig(BaseModel):
    source: SourceConfig
    sql_schema: list[Path] = Field(alias="schema")
    export: list[Path]


def tasktree_from_config(config: SourceConfig) -> TaskTree:
    if config.type == "scival":
        return scival.create_tasks(config)
    elif config.type == "pure":
        return pure.create_tasks(config)
    else:
        raise RuntimeError("Unknown type " + config.type)


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-c", "--config", type=Path)
    argparser.add_argument("output", type=Path)
    args = argparser.parse_args()

    with Path(args.config).open() as config_file:
        config = CsmlConfig.model_validate(yaml.safe_load(config_file))

    temp_db: Path = args.output.with_stem(args.output.stem + ".tmp")
    temp_db.unlink(True)

    with sqlite3.connect(str(temp_db)) as conn:
        conn.execute("PRAGMA foreign_keys = 1")
        TaskIndex(tasktree_from_config(config.source)).full_sequence().run(conn)

    args.output.unlink(True)
    with sqlite3.connect(str(args.output)) as conn:
        conn.execute("PRAGMA foreign_keys = 1")

        for script_path in track(
            config.sql_schema, description="Preparing schema...", console=console
        ):
            console.print(f"Executing [bold cyan]{script_path}")
            conn.executescript(script_path.read_text())

        conn.execute(sql_adapter.format("ATTACH DATABASE {} AS 'tmp'", str(temp_db)))
        for script_path in track(
            config.export, description="Exporting data...", console=console
        ):
            console.print(f"Executing [bold cyan]{script_path}")
            conn.executescript(sql_environment.render(script_path.read_text(), slice=0))
