CREATE TABLE {{table}}(
    id_record INT REFERENCES {{records}}(id_record),
    num_topics INT,
    name_topics TEXT,
    ProminencePercentile REAL,
    iscluster INT
);

INSERT INTO {{table}}
SELECT
    id_record,
    CAST(topic_number AS INT),
    COALESCE(NULLIF(topic_name, '-'), 'NoN'), 
    CAST(topic_prominence_percentile AS REAL),
    0
FROM {{records}}
UNION
SELECT
    id_record,
    CAST(topic_cluster_number AS INT),
    COALESCE(NULLIF(topic_cluster_name, '-'), 'NoN'), 
    CAST(topic_cluster_prominence_percentile AS REAL),
    1
FROM {{records}};
