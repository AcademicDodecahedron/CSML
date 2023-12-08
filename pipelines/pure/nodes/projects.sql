SELECT 

SELECT uuid, type_classification, title FROM {{raw}}
GROUP BY uuid;
