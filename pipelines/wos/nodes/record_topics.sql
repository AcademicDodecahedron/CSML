CREATE TABLE {{table}}(
    id_record_topic INTEGER PRIMARY KEY AUTOINCREMENT,
    id_record INT REFERENCES {{records}}(id_record),
    research_area TEXT,
    iscluster INT
);

INSERT INTO {{table}}(id_record, research_area, iscluster)
SELECT
    id_record,
    research_area,
    0 AS iscluster
FROM {{records}} AS records
JOIN {{incites}} AS incites ON records.num_record = incites.num_record
WHERE footer__schema = 'Citation Topics - Micro';
