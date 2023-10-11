CREATE TABLE {{table}}(
    num_record_ids TEXT,
    type_record_ids INT,
    id_record INT REFERENCES {{records}}(id_record)
);

INSERT INTO {{table}}
SELECT eid, 2, id_record FROM {{records}}
UNION
SELECT sgr, 2, id_record FROM {{records}};
