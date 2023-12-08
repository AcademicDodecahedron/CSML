{% set columns=[Column("org_id", "INT REFERENCES {{orgs}}(org_id)")] %}

UPDATE {{table}} AS this
SET org_id = orgs.org_id
FROM {{orgs}} AS orgs
WHERE this.org_uuid = orgs.uuid;
