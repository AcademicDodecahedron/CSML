from pathlib import Path
import glob
from typing import Callable, Iterable, Optional

from lib import console

from .wos import *
from .incites import *


def load_files_glob(pattern: Optional[str], fn: Callable[[Path], Iterable[dict]]):
    def wrapper():
        if not pattern:
            return

        for file_path in map(Path, glob.iglob(pattern, recursive=True)):
            console.log("Reading", file_path)
            filename = file_path.name

            for row in fn(file_path):
                yield {**row, "filename": filename}

    return wrapper
