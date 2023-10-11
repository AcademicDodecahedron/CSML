CREATE TABLE {{table}}(
    field_name TEXT,
    value_category TEXT,
    type_category INT,
    id_record INT REFERENCES {{records}}(id_record)
);

INSERT INTO {{table}}
SELECT * FROM {{split}}
UNION
SELECT
    'filename',
    raw.filename,
    37,
    records.id_record
FROM {{records}} AS records
JOIN {{records_raw}} AS raw
ON records.eid = raw.eid;
