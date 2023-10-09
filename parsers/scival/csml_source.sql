CREATE TABLE {{table}}(
    id_source INTEGER PRIMARY KEY AUTOINCREMENT,
    source_title TEXT,
    issn TEXT,
    num_source TEXT,
    source_type TEXT
);

INSERT INTO {{table}}(source_title, issn, num_source, source_type)
SELECT DISTINCT scopus_source_title, issn, num_source, source_type FROM {{records}};
