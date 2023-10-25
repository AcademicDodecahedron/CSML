CREATE TABLE {{table}}(
    id_record INT REFERENCES {{records}}(id_record),
    filename TEXT
);

INSERT INTO {{table}}
SELECT id_record, filename
FROM {{records}} AS records
JOIN {{wos}} AS wos ON records.num_record = wos.num_record
UNION
SELECT id_record, filename
FROM {{records}} AS records
JOIN {{incites}} AS incites ON records.num_record = incites.num_record;
