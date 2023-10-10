import sqlite3
import yaml
from pathlib import Path
from pydantic import BaseModel, Field
from argparse import ArgumentParser

from lib import Task
from pipelines import scival, pure, SourceConfig


class CsmlConfig(BaseModel):
    source: SourceConfig
    sql_schema: list[Path] = Field(alias="schema")


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
