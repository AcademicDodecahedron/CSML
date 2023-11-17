import re
from lib import ValueColumnRendered


def normalize_name(name: str) -> str:
    return re.sub(r"[^\w]+", "_", name).lower()


WOS_COLUMNS = list(
    map(
        lambda name: ValueColumnRendered(name, "TEXT"),
        [
            # fmt: off
            "PT", "AU", "BA", "BE", "GP", "AF", "BF", "CA", "TI", "SO", "SE", "BS", "LA",
            "DT", "CT", "CY", "CL", "SP", "HO", "DE", "ID", "AB", "C1", "C3", "RP", "EM",
            "RI", "OI", "FU", "FP", "FX", "CR", "NR", "TC", "Z9", "U1", "U2", "PU", "PI",
            "PA", "SN", "EI", "BN", "J9", "JI", "PD", "PY", "VL", "IS", "PN", "SU", "SI",
            "MA", "BP", "EP", "AR", "DI", "DL", "D2", "EA", "PG", "WC", "WE", "SC", "GA",
            "PM", "OA", "HC", "HP", "DA", "UT"
            # fmt: on
        ],
    )
)

INCITES_COLUMNS = list(
    map(
        lambda name: ValueColumnRendered(normalize_name(name), "TEXT"),
        [
            "Accession Number",
            "DOI",
            "Pubmed ID",
            "Article Title",
            "Authors",
            "Source",
            "Research Area",
            "Document Type",
            "Volume",
            "Issue",
            "Pages",
            "Publication Date",
            "Times Cited",
            "Link",
            "Journal Expected Citations",
            "Category Expected Citations",
            "Journal Normalized Citation Impact",
            "Category Normalized Citation Impact",
            "Percentile in Subject Area",
            "Journal Impact Factor",
        ],
    )
)
