{% set columns = [
    Column("search_path", "TEXT"),
    Column("level", "INT"),
] -%}

UPDATE {{table}} AS this SET
    search_path = tmp.search_path,
    level = tmp.level
FROM (
    WITH RECURSIVE tree(org_id, uuid, search_path, level) AS (
        SELECT org_id, uuid, '/' || org_id, 1 FROM {{table}}
        WHERE parent_uuid IS NULL
        UNION ALL
        SELECT this.org_id, this.uuid, tree.search_path || '/' || this.org_id, tree.level + 1
        FROM {{table}} AS this
            JOIN tree ON this.parent_uuid = tree.uuid
    )
    SELECT org_id, search_path, level
    FROM tree
) AS tmp
WHERE this.org_id = tmp.org_id;
