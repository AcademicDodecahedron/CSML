CREATE TABLE {{table}}(
    id_record INT PRIMARY KEY REFERENCES {{records}}(id_record),
    incites_times_cited INT,
    incites_journal_expected_citations REAL,
    incites_category_expected_citations REAL,
    incites_journal_normalized_citation_impact REAL,
    incites_category_normalized_citation_impact REAL,
    incites_percentile_in_subject_area REAL,
    incites_journal_impact_factor REAL
);

INSERT INTO {{table}}
SELECT
    id_record, 
    CAST(NULLIF(times_cited, 'n/a') AS INT),
    CAST(NULLIF(journal_expected_citations, 'n/a') AS REAL),
    CAST(NULLIF(category_expected_citations, 'n/a') AS REAL),
    CAST(NULLIF(journal_normalized_citation_impact, 'n/a') AS REAL),
    CAST(NULLIF(category_normalized_citation_impact, 'n/a') AS REAL),
    CAST(NULLIF(percentile_in_subject_area, 'n/a') AS REAL),
    CAST(NULLIF(journal_impact_factor, 'n/a') AS REAL)
FROM {{records}} AS r
JOIN {{incites}} AS i ON footer__schema = 'Web of Science'
    AND r.num_record = i.num_record
GROUP BY id_record;
