import json
import typer
from typing import Type, Annotated, Optional
from pydantic import BaseModel

from lib import console


def config_schema_for_class(config: Type[BaseModel]):
    def config_schema(
        output: Annotated[
            Optional[typer.FileTextWrite], typer.Option("--output", "-o")
        ] = None
    ):
        schema = config.model_json_schema()

        if output:
            json.dump(schema, output, indent=4)
        else:
            console.print_json(data=schema, indent=4)

    return config_schema
