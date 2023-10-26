import re
import html
from typing import Optional


def make_database_record(ut: str, we: Optional[str], pm: Optional[str]):
    if we:
        for database_full in html.unescape(we).split("; "):
            match = re.search(r"\((.+)\)$", database_full)
            if not match:
                raise RuntimeError("no parentheses in WE", database_full)

            database_short = match.group(1).replace("-", "").replace("&", "")
            yield {"name_database": database_short, "num_record_in_database": ut}

    if pm:
        yield {"name_database": "PubMed", "num_record_in_database": pm}
