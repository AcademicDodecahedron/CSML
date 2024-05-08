CREATE TABLE {{table}}(
    id_record INT REFERENCES {{records}},
    type_category INT,
    value_category TEXT
);

INSERT INTO {{table}}
SELECT
    id_record,
    37,
    filename
FROM {{records}} JOIN {{xmls}} ON {{records}}.xml_id = {{xmls}}.xml_id;

INSERT INTO {{table}}
SELECT id_record, 10, grnti
FROM {{records}}
WHERE grnti IS NOT NULL;

INSERT INTO {{table}}
SELECT
    id_record,
    38,
    vak || '#' || rsci || '#' || wos || '#' || scopus
FROM {{records}};
