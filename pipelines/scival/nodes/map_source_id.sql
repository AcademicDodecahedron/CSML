{% set columns = [Column("id_source", "INT REFERENCES {{sources}}(id_source)")] %}

UPDATE {{table}} AS this
SET id_source = sources.id_source
FROM {{sources}} AS sources
WHERE this.num_source = sources.num_source
