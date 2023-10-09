{% set columns = [Column("id_source", "INT REFERENCES {{sources}}(id_source)")] %}

UPDATE {{table}} AS this
SET id_source = sources.id_source
FROM {{sources}} AS sources
WHERE this.scopus_source_title = sources.source_title
AND this.issn = sources.issn
AND this.num_source = sources.num_source
AND this.source_type = sources.source_type;
