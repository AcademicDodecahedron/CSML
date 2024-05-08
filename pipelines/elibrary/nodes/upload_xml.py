from io import TextIOWrapper
from pathlib import Path
from typing import Optional
from zipfile import ZipFile


def upload_xml(path: Path, encoding: Optional[str] = None):
    if path.suffix == ".zip":
        with ZipFile(path) as archive:
            for file_info in archive.filelist:
                yield {
                    "xmlname": f"{path.name}/{file_info.filename}",
                    "xml": TextIOWrapper(archive.open(file_info), encoding).read(),
                }
    elif path.suffix == ".xml":
        yield {"xmlname": path.name, "xml": path.read_text(encoding)}
