CREATE TABLE {{table}}(
    id_project INTEGER PRIMARY KEY AUTOINCREMENT,
    uuid TEXT UNIQUE,
    type_classification TEXT,
    title TEXT
);

INSERT INTO {{table}}(uuid, type_classification, title)
SELECT uuid, type_classification, title FROM {{record_projects}}
GROUP BY uuid;
