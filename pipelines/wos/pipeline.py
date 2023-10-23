from pathlib import Path
from typing import Literal
from pydantic import BaseModel

from lib import (
    TaskTree,
    table,
    placeholder,
    with_args,
    MapToNewTable,
    ValueColumn,
    CreateTableSql,
    AddColumnsSql,
    Column,
)
from .nodes.load_txt import load_txts_glob


class WosConfig(BaseModel):
    type: Literal["wos"]
    glob: str

    def create_tasks(self):
        return create_tasks(self)


def create_tasks(config: WosConfig) -> TaskTree:
    table_records_raw = table("records_raw")
    table_records = table("records")

    parent_folder = Path(__file__).parent

    record_columns = list(
        map(
            ValueColumn.render,
            [
                ValueColumn("num_record_raw", "TEXT", placeholder("UT")),
                ValueColumn("source_title", "TEXT", placeholder("SO")),
            ],
        )
    )

    return {
        "records": {
            "load": MapToNewTable(
                table=table_records_raw,
                columns=[ValueColumn("filename", "TEXT"), *record_columns],
                fn=with_args(load_txts_glob, pattern=config.glob),
            ),
            "dedupe": CreateTableSql(
                table=table_records,
                sql=parent_folder.joinpath("./nodes/dedupe.sql").read_text(),
                params={"raw": table_records_raw, "record_columns": record_columns},
            ),
            "num_record": AddColumnsSql(
                table=table_records,
                columns=[Column("num_record", "TEXT")],
                sql="""\
                UPDATE {{table}}
                SET num_record = REPLACE(num_record_raw, 'WOS:', '')""",
            ),
        }
    }
