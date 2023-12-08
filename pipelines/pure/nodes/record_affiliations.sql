CREATE TABLE {{table}}(
    id_record_affiliation INTEGER PRIMARY KEY AUTOINCREMENT,
    id_record INT REFERENCES {{records}}(id_record),
    org_uuid TEXT,
    full_address TEXT,
    only_record_level INT
);

INSERT INTO {{table}}(id_record, org_uuid, full_address, only_record_level)
WITH joined AS (
    SELECT id_record, org_uuid, full_address, only_record_level FROM {{author_affiliations}}
    UNION
    SELECT id_record, org_uuid, full_address, only_record_level FROM {{org_units}}
)
SELECT * FROM joined
GROUP BY id_record, org_uuid;
