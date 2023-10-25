{% set columns = [
    Column("id_record_affiliation", "INT REFERENCES {{record_affiliations}}(id_record_affiliation)")
] -%}

UPDATE {{table}} AS this
SET id_record_affiliation = aff.id_record_affiliation
FROM {{record_affiliations}} AS aff
WHERE this.id_record = aff.id_record
AND this."order" = aff."order";
