from pathlib import Path
from typing import Literal, Optional
from pydantic import BaseModel

from lib import (
    TaskTree,
    table,
    rename_output,
    MapToNewTable,
    ValueColumn,
    AddColumnsSql,
    Column,
    CreateTableSql,
)
from .nodes.loaders import load_files_glob, load_wos, load_incites
from .fields import WOS_COLUMNS, INCITES_COLUMNS, normalize_name

__dir__ = Path(__file__).parent


class WosConfig(BaseModel):
    type: Literal["wos"]
    wos_glob: str
    incites_glob: Optional[str]

    def create_tasks(self):
        return create_tasks(self)


def create_tasks(config: WosConfig) -> TaskTree:
    table_wos = table("wos")
    table_incites = table("incites")
    table_records = table("records")

    return {
        "wos": {
            "load": MapToNewTable(
                table=table_wos,
                columns=[ValueColumn("filename", "TEXT"), *WOS_COLUMNS],
                fn=load_files_glob(config.wos_glob, load_wos),
            ),
            "num_record": AddColumnsSql(
                table=table_wos,
                columns=[Column("num_record", "TEXT")],
                sql="""\
                UPDATE {{table}}
                SET num_record = REPLACE(UT, 'WOS:', '')""",
            ),
        },
        "incites": {
            "load": MapToNewTable(
                table=table_incites,
                columns=[
                    ValueColumn("filename", "TEXT"),
                    ValueColumn("footer__schema", "TEXT"),
                    *INCITES_COLUMNS,
                ],
                fn=rename_output(
                    load_files_glob(config.incites_glob, load_incites), normalize_name
                ),
            ),
            "num_record": AddColumnsSql(
                table=table_incites,
                columns=[Column("num_record", "TEXT")],
                sql="""\
                UPDATE {{table}}
                SET num_record = REPLACE(accession_number, 'WOS:', '')""",
            ),
        },
        "records": CreateTableSql(
            table=table_records,
            sql=__dir__.joinpath("./nodes/records.sql").read_text(),
            params={"wos": table_wos, "incites": table_incites},
        ),
    }
