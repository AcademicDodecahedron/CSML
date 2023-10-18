import sys
import yaml
from argparse import ArgumentParser
from pathlib import Path
from returns.maybe import Maybe
from rich.pretty import Pretty

from lib.console import console
from pipelines.scival import ScivalConfig, HeaderLength, incites_csv_reader


def load_csv_fieldnames(path: Path, header_length: HeaderLength) -> set[str]:
    with path.open(encoding="utf-8-sig") as csv_file:
        return set(
            Maybe.from_optional(
                incites_csv_reader(csv_file, header_length).fieldnames
            ).unwrap()
        )


def load_file_or_folder_fieldnames(path: Path, header_length: HeaderLength):
    csv_paths = iter(path.glob("*.csv") if path.is_dir() else [path])

    csv_path = next(csv_paths)
    fieldnames = load_csv_fieldnames(csv_path, header_length)

    for csv_path_other in csv_paths:
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


def load_config(path: Path):
    with path.open() as config_file:
        return ScivalConfig.model_validate(yaml.safe_load(config_file)["source"])


def parse_args():
    def parse_header_length(value: str) -> HeaderLength:
        return "auto" if value == "auto" else int(value)

    argparser = ArgumentParser()
    argparser.add_argument(
        "csv",
        type=Path,
        help="csv to read new field names from (can be a file or folder)",
    )
    argparser.add_argument(
        "header_length",
        type=parse_header_length,
        help="number of lines to skip, or 'auto' for auto-detection",
    )

    argparser.add_argument(
        "-c",
        "--config",
        type=Path,
        help="config with the old mapping",
    )

    return argparser.parse_args()


if __name__ == "__main__":
    args = parse_args()

    fields_old = (
        Maybe.from_optional(args.config)
        .map(load_config)
        .map(lambda config: set(config.fields.keys()))
        .value_or(set())
    )
    fields_new = load_file_or_folder_fieldnames(args.csv, args.header_length)

    console.print("[bold]Missing fields:")
    for missing in fields_old - fields_new:
        console.print(f"[red]{missing}")

    console.print("\n[bold]New fields:")
    for new_field in fields_new - fields_old:
        console.print(f"[cyan]{new_field}")
