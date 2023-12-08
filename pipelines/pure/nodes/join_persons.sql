CREATE TABLE {{table}}(
    id_person INTEGER PRIMARY KEY AUTOINCREMENT,
    filename      TEXT,
    type_pure_person INT,
    uuid          TEXT UNIQUE,
    first_name    TEXT,
    last_name     TEXT,
    date_of_birth TEXT,
    year_of_birth INT,
    orcid         TEXT,
    year          INT,
    education     TEXT,
    qualification TEXT,
    name_variants TEXT,
    ids           TEXT,
    staff_org_ass TEXT
);

INSERT INTO {{table}}(
    filename,
    uuid,
    type_pure_person,
    first_name,
    last_name,
    date_of_birth,
    year_of_birth,
    orcid,
    year,
    education,
    qualification,
    name_variants,
    ids,
    staff_org_ass
)
SELECT 
    filename,
    uuid,
    1,
    first_name,
    last_name,
    date_of_birth,
    year_of_birth,
    orcid,
    year,
    education,
    qualification,
    name_variants,
    ids,
    staff_org_ass
FROM {{internalperson}}
GROUP BY uuid;

INSERT INTO {{table}}(filename, type_pure_person, uuid, first_name, last_name)
SELECT filename, 2, external_person_uuid, first_name, last_name FROM {{authors}}
JOIN {{records}} ON {{records}}.id_record = {{authors}}.id_record
WHERE external_person_uuid IS NOT NULL
GROUP BY external_person_uuid;
