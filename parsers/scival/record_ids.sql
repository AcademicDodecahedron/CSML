CREATE TABLE {{table}}(
    num_record_ids TEXT,
    type_record_ids INT
);

INSERT INTO {{table}}
SELECT eid, 2 FROM {{records}}
UNION
SELECT sgr, 2 FROM {{records}};
