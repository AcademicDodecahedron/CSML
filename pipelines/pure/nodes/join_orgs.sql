CREATE TABLE {{table}}(
    org_id INTEGER PRIMARY KEY AUTOINCREMENT,
    type_pure_org INT,
    filename TEXT,
    uuid TEXT UNIQUE,
    parent_uuid TEXT,
    kind_pure_org TEXT,
    name_pure_org TEXT,
    name_pure_org_eng TEXT,
    country_org TEXT,
    city_org TEXT,
    ids TEXT
);

INSERT INTO {{table}}(
    type_pure_org,
    filename,
    uuid,
    parent_uuid,
    name_pure_org,
    name_pure_org_eng,
    ids
) SELECT
    1,
    filename,
    uuid,
    parent_uuid,
    name_pure_org,
    name_pure_org_eng,
    ids
FROM {{internalorg}}
GROUP BY uuid;

INSERT INTO {{table}}(
    type_pure_org,
    filename,
    uuid,
    parent_uuid,
    kind_pure_org,
    name_pure_org,
    name_pure_org_eng,
    country_org,
    city_org,
    ids
) SELECT
    2,
    filename,
    uuid,
    parent_uuid,
    kind_pure_org,
    name_pure_org,
    name_pure_org_eng,
    country_org,
    city_org,
    ids
FROM {{externalorg}}
GROUP BY uuid;

INSERT INTO {{table}}(type_pure_org, filename, uuid, name_pure_org)
SELECT 2, filename, org_uuid, full_address FROM {{record_affiliations}} aff
JOIN {{records}} ON aff.id_record = {{records}}.id_record
WHERE NOT EXISTS (
    SELECT 1 FROM {{table}} WHERE {{table}}.uuid = aff.org_uuid
)
GROUP BY org_uuid;
