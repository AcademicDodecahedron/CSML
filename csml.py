import sqlite3
import yaml
from pathlib import Path
from pydantic import BaseModel, Field
from argparse import ArgumentParser
from sqlglot.expressions import Literal

from lib import Task, SqlEnvironment
from pipelines import scival, pure, SourceConfig


class CsmlConfig(BaseModel):
    source: SourceConfig
    sql_schema: list[Path] = Field(alias="schema")
    export: list[Path]


def pipeline_from_config(config: SourceConfig) -> dict:
    if config.type == "scival":
        return scival.create_tasks(config)
    elif config.type == "pure":
        return pure.create_tasks(config)
    else:
        raise RuntimeError("Unknown type " + config.type)


def run_sequence(sequence: dict, conn: sqlite3.Connection):
    for name, task in sequence.items():
        print("Running", name)
        if isinstance(task, Task):
            task.run(conn)
            conn.commit()
        else:
            run_sequence(task, conn)


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-c", "--config", type=Path)
    argparser.add_argument("output", type=Path)
    args = argparser.parse_args()

    with Path(args.config).open() as config_file:
        config = CsmlConfig.model_validate(yaml.safe_load(config_file))

    # TaskIndex.resolve(pipeline_from_config(config.source))

    temp_db: Path = args.output.with_stem(args.output.stem + ".tmp")
    temp_db.unlink(True)

    with sqlite3.connect(str(temp_db)) as conn:
        run_sequence(pipeline_from_config(config.source), conn)

    args.output.unlink(True)
    with sqlite3.connect(str(args.output)) as conn:
        conn.execute("PRAGMA foreign_keys = 1")

        for script_path in config.sql_schema:
            print("Executing ", script_path)
            conn.executescript(script_path.read_text())

        conn.execute(
            "ATTACH DATABASE {} AS 'tmp'".format(Literal.string(temp_db).sql())
        )
        for script_path in config.export:
            print("Executing ", script_path)
            conn.executescript(
                SqlEnvironment.default.from_string(script_path.read_text()).render(
                    slice=0
                )
            )
