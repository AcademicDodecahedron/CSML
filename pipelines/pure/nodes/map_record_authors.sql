{% set columns = [Column("id_person", "INT REFERENCES {{person}}(id_person)")] %}

UPDATE {{table}} AS this
SET id_person = person.id_person
FROM {{person}} AS person
WHERE COALESCE(person_uuid, external_person_uuid) = person.uuid;
