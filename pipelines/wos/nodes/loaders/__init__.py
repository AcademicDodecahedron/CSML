from functools import wraps
from pathlib import Path
import glob
from typing import Callable, Iterable, Optional

from lib import console, track, OneToMany

from .wos import *
from .incites import *


def load_files_glob(
    pattern: Optional[str], fn: Callable[[Path], Iterable[dict]]
) -> OneToMany:
    @wraps(fn)
    def wrapper():
        if pattern:
            paths = list(map(Path, glob.iglob(pattern, recursive=True)))
            for file_path in track(paths, description="Reading..."):
                console.log("Reading", file_path)
                filename = file_path.name

                for row in fn(file_path):
                    yield {**row, "filename": filename}

    return wrapper
