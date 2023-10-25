CREATE TABLE {{table}}(
    id_record INT REFERENCES {{records}}(id_record),
    field_name TEXT,
    type_category INT,
    value_category TEXT
);

INSERT INTO {{table}}
SELECT 
    id_record,
    field_name,
    type_category,
    value_category
FROM {{split}};

INSERT INTO {{table}}
SELECT
    id_record,
    'filename',
    37,
    filename
FROM {{filename}};

INSERT INTO {{table}}
SELECT DISTINCT
    id_record,
    'org_name',
    23,
    org_name
FROM {{affiliations}};

INSERT INTO {{table}}
SELECT DISTINCT
    id_record,
    'country',
    25,
    country
FROM {{affiliations}};
