CREATE TABLE {{table}}(
    id_record_affiliation INTEGER PRIMARY KEY AUTOINCREMENT,
    id_record INT REFERENCES {{records}}(id_record),
    "order" INT,
    full_address TEXT
);

INSERT INTO {{table}}(id_record, "order", full_address)
SELECT id_record, "order", full_address
FROM {{rel_aff}}
GROUP BY id_record, "order";
