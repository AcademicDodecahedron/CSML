import yaml
from pathlib import Path
from pydantic import BaseModel, Field
from argparse import ArgumentParser

from pipelines import scival, pure, SourceConfig


class CsmlConfig(BaseModel):
    source: SourceConfig
    sql_schema: list[Path] = Field(alias="schema")


def pipeline_from_config(config: SourceConfig):
    if config.type == "scival":
        return scival.create_tasks(config)
    elif config.type == "pure":
        return pure.create_tasks(config)


if __name__ == "__main__":
    argparser = ArgumentParser()
    argparser.add_argument("-c", "--config", type=Path)
    argparser.add_argument("output", type=Path)
    args = argparser.parse_args()

    with Path(args.config).open() as config_file:
        config = CsmlConfig.model_validate(yaml.safe_load(config_file))

    TaskIndex.resolve(pipeline_from_config(config.source))
