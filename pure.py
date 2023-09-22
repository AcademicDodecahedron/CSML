from pathlib import Path

from parsers.internalorgs import parse_internalorg_folder
from templates import table, ValueColumn
from tasks import MapToNewTable

TABLE_internalorgs = table("internalorgs")


def with_args(fn, **add_kwargs):
    def wrapper(**kwargs):
        yield from fn(**kwargs, **add_kwargs)

    return wrapper


task = MapToNewTable(
    table=TABLE_internalorgs,
    columns=[
        "org_id INTEGER PRIMARY KEY AUTOINCREMENT DEFAULT",
        ValueColumn("uuid", "TEXT DEFAULT"),
        ValueColumn("parent_uuid", "TEXT"),
        ValueColumn("kind_pure_org", "TEXT"),
        ValueColumn("name_pure_org", "TEXT"),
        ValueColumn("name_pure_org_eng", "TEXT"),
        ValueColumn("ids", "TEXT"),
    ],
    fn=with_args(parse_internalorg_folder, xml=Path("../rez_internalorg_20220926/")),
)
for name, script in task.scripts.items():
    print(name)
    print(script)
    print()
