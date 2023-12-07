CREATE TABLE {{table}}(
    org_id INTEGER PRIMARY KEY AUTOINCREMENT,
    type_pure_org INT,
    filename TEXT,
    uuid TEXT,
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
FROM {{internalorg}};

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
FROM {{externalorg}};
