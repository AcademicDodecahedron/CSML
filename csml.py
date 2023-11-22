import typer
import sqlite3
import yaml
import json
from pathlib import Path
from pydantic import BaseModel, Field
from typing import Annotated, Optional

from lib import TaskIndex, sql_environment, sql_adapter, console, track
from pipelines import SourceConfig
from check_scival_fields import check_scival_fields


class CsmlConfig(BaseModel):
    source: SourceConfig
    sql_schema: list[Path] = Field(alias="schema")
    export: list[Path]


app = typer.Typer(add_completion=False, no_args_is_help=True)


@app.command(no_args_is_help=True)
def run(
    output: Annotated[Path, typer.Argument(help="output sqlite database")],
    config_file: Annotated[
        typer.FileText, typer.Option("--config", "-c", help="YAML config file")
    ],
    slice: Annotated[int, typer.Option("--slice", "-s", help="id_slice value")] = 0,
    continue_: Annotated[
        bool, typer.Option("--continue", help="don't clear the temporary database")
    ] = False,
):
    config = CsmlConfig.model_validate(yaml.safe_load(config_file))

    temp_db = output.with_stem(output.stem + ".tmp")
    if not continue_:
        temp_db.unlink(True)

    with sqlite3.connect(str(temp_db)) as conn:
        conn.execute("PRAGMA foreign_keys = 1")
        conn.execute("BEGIN")
        TaskIndex(config.source.create_tasks()).full_sequence().run(conn)

    output.unlink(True)
    with sqlite3.connect(str(output)) as conn:
        conn.execute("PRAGMA foreign_keys = 1")

        for script_path in track(config.sql_schema, description="Preparing schema..."):
            console.print(f"Executing [bold cyan]{script_path}")
            conn.executescript(script_path.read_text())

        conn.execute(sql_adapter.format("ATTACH DATABASE {} AS 'tmp'", str(temp_db)))
        for script_path in track(config.export, description="Exporting data..."):
            console.print(f"Executing [bold cyan]{script_path}")
            conn.executescript(
                sql_environment.render(script_path.read_text(), slice=slice)
            )


@app.command()
def config_schema(
    output: Annotated[
        Optional[typer.FileTextWrite], typer.Option("--output", "-o")
    ] = None
):
    schema = CsmlConfig.model_json_schema()
    if output:
        json.dump(schema, output, indent=4)
    else:
        console.print_json(data=schema, indent=4)


app.command(no_args_is_help=True)(check_scival_fields)


if __name__ == "__main__":
    app()
