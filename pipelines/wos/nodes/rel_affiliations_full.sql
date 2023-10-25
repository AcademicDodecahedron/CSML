CREATE TABLE {{table}}(
    id_record_affiliation INT REFERENCES {{affiliations}}(id_record_affiliation),
    id_record_author INT REFERENCES {{authors}}(id_record_author),
    "order" INT
);

INSERT INTO {{table}}
SELECT id_record_affiliation, id_record_author, "order"
FROM {{raw}} AS rel
JOIN {{authors}} AS authors ON rel.id_record = authors.id_record AND (
    rel.author_name IS NULL OR rel.author_name = authors.full_name
);
