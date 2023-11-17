import glob
from pathlib import Path
from functools import wraps
from typing import Optional

from lib import console, track, OneToMany


def load_files_glob(
    pattern: Optional[str], fn: OneToMany, arg_name: str = "path"
) -> OneToMany:
    @wraps(fn)
    def wrapper():
        if pattern:
            paths = list(map(Path, glob.iglob(pattern, recursive=True)))
            for file_path in track(paths, description="Reading..."):
                console.log("Reading", file_path)
                filename = file_path.name

                for row in fn(**{arg_name: file_path}):
                    yield {**row, "filename": filename}

    return wrapper
