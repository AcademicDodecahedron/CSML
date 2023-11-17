import inspect
from pathlib import Path


def folder() -> Path:
    return Path(inspect.stack()[1].filename).parent
