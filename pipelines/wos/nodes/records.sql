CREATE TABLE {{table}}(
    id_record INTEGER PRIMARY KEY AUTOINCREMENT,
    source_priority INT,
    num_record TEXT UNIQUE,
    ut TEXT,
    doi TEXT,
    document_type TEXT,
    source_title TEXT,
    source_type TEXT,
    author_au TEXT,
    author_af TEXT,
    lang_document TEXT,
    affiliation_c1 TEXT,
    refs_count INT,
    cited_from_record INT,
    issn TEXT,
    year_publ TEXT,
    category_wc TEXT,
    category_sc TEXT,
    database_we TEXT,
    database_pm TEXT
);

INSERT INTO {{table}}(
    source_priority,
    num_record,
    ut,
    doi,
    document_type,
    source_title,
    source_type,
    author_au,
    author_af,
    lang_document,
    affiliation_c1,
    refs_count,
    cited_from_record,
    issn,
    year_publ,
    category_wc,
    category_sc,
    database_we,
    database_pm
)
with joined as (
    select
        ROWID as row_priority,
        1 as source_priority,

        num_record,
        UT,
        NULLIF(DI, '') as doi,
        NULLIF(DT, '') as document_type,
        NULLIF(J9, '') as source_title,

        NULLIF(PT, '') as source_type,
        NULLIF(AU, '') as author_au,
        NULLIF(AF, '') as author_af,
        NULLIF(LA, '') as lang_document,
        NULLIF(C1, '') as affiliation_c1,
        CAST(NULLIF(NR, '') AS INT) as refs_count,
        CAST(NULLIF(TC, '') AS INT) as cited_from_record,
        REPLACE(COALESCE(NULLIF(SN, ''), NULLIF(EI, '')), '-', '') as issn,
        NULLIF(PY, '') as year_publ,
        NULLIF(WC, '') as category_wc,
        NULLIF(SC, '') as category_sc,
        NULLIF(WE, '') as database_we,
        NULLIF(PM, '') as database_pm
    from {{wos}}
    union
    select
        ROWID as row_priority,
        2 as source_priority,

        num_record,
        accession_number,
        NULLIF(doi, 'n/a'),
        document_type,
        source as source_title,

        NULL as source_type,
        NULL as author_au,
        NULL as author_af,
        NULL as lang_document,
        NULL as affiliation_c1,
        NULL as refs_count,
        NULL as cited_from_record,
        NULL as issn,
        NULL as year_publ,
        NULL as category_wc,
        NULL as category_sc,
        NULL as database_we,
        NULL as database_pm
    from {{incites}}
    where footer__schema = 'Web of Science'
)
select
    source_priority,
    num_record,
    ut,
    doi,
    document_type,
    source_title,
    source_type,
    author_au,
    author_af,
    lang_document,
    affiliation_c1,
    refs_count,
    cited_from_record,
    issn,
    year_publ,
    category_wc,
    category_sc,
    database_we,
    database_pm
from joined
group by num_record
having min(source_priority) and min(row_priority);
