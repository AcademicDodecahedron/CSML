{% set columns = [Column("id_project", "INT REFERENCES {{projects}}(id_project)")] %}

UPDATE {{table}} AS this
SET id_project = projects.id_project
FROM {{projects}} AS projects
WHERE this.uuid = projects.uuid;
