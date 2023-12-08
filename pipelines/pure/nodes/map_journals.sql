{% set columns = [Column("source_id", "INT REFERENCES {{journals}}(source_id)")] %}

UPDATE {{table}} AS this
SET source_id = journals.source_id
FROM {{journals}} AS journals
WHERE this.journal_uuid = journals.uuid;
