import typer
import sys
from glob import iglob
from typing import Annotated, Iterable, Optional
import yaml
from pathlib import Path
from returns.maybe import Maybe

from lib.console import console
from pipelines.scival import ScivalConfig, HeaderLength, incites_csv_reader


def load_csv_fieldnames(path: Path, header_length: HeaderLength) -> set[str]:
    with path.open(encoding="utf-8-sig") as csv_file:
        return set(
            Maybe.from_optional(
                incites_csv_reader(csv_file, header_length).fieldnames
            ).unwrap()
        )


def load_fieldnames_check_same(paths: Iterable[Path], header_length: HeaderLength):
    paths_iter = iter(paths)

    csv_path = next(paths_iter)
    fieldnames = load_csv_fieldnames(csv_path, header_length)

    for csv_path_other in paths_iter:
        fieldnames_other = load_csv_fieldnames(csv_path_other, header_length)

        if fieldnames != fieldnames_other:
            console.print("[bold red]CSVs have different fieldnames!")
            console.print()
            console.print(f"[bold]{csv_path}:")
            console.print(str(fieldnames))
            console.print()
            console.print(f"[bold]{csv_path_other}:")
            console.print(str(fieldnames_other))

            sys.exit(1)

    return fieldnames


class HeaderLengthArg:
    def __init__(self, raw: str) -> None:
        self.value: HeaderLength = "auto" if raw == "auto" else int(raw)


def check_scival_fields(
    glob: Annotated[str, typer.Argument(help="Glob to find CSVs")],
    header_length: Annotated[
        HeaderLengthArg,
        typer.Argument(
            parser=HeaderLengthArg,
            metavar="HEADERLENGTH",
            help="number of lines to skip, or 'auto' for auto-detection",
        ),
    ],
    config: Annotated[
        Optional[typer.FileText],
        typer.Option("-c", "--config", help="YAML config file"),
    ] = None,
):
    fields_old = (
        Maybe.from_optional(config)
        .map(lambda file: ScivalConfig.model_validate(yaml.safe_load(file)["source"]))
        .map(lambda config: set(config.fields.keys()))
        .value_or(set())
    )
    fields_new = load_fieldnames_check_same(map(Path, iglob(glob)), header_length.value)

    console.print("[bold]Missing fields:")
    for missing in fields_old - fields_new:
        console.print(f"[red]{missing}")

    console.print("\n[bold]New fields:")
    for new_field in fields_new - fields_old:
        console.print(f"[cyan]{new_field}")


__all__ = ["check_scival_fields"]
