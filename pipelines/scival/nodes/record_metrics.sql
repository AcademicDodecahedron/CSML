CREATE TABLE {{table}}(
    id_record INT REFERENCES {{records}}(id_record),
    field_weighted_view_impact REAL,
    field_weighted_citation_impact REAL,
    views INT,
    outputs_in_top_citation_percentiles_per_percentile INT,
    field_weighted_outputs_in_top_citation_percentiles_per_percentile INT,
    snip REAL,
    cite_score REAL,
    sjr REAL,
    number_of_authors INT,
    snip_percentile INT,
    cite_score_percentile INT,
    sjr_percentile INT
);

INSERT INTO {{table}}
SELECT
    id_record,
    CAST(NULLIF(field_weighted_view_impact, '-') AS REAL),
    CAST(NULLIF(field_weighted_citation_impact, '-') AS REAL),
    CAST(NULLIF(views, '-') AS INT),
    CAST(NULLIF(outputs_in_top_citation_percentiles_per_percentile, '-') AS INT),
    CAST(NULLIF(field_weighted_outputs_in_top_citation_percentiles_per_percentile, '-') AS INT),
    CAST(NULLIF(snip, '-') AS REAL),
    CAST(NULLIF(cite_score, '-') AS REAL),
    CAST(NULLIF(sjr, '-') AS REAL),
    CAST(NULLIF(number_of_authors, '-') AS INT),
    CAST(NULLIF(snip_percentile, '-') AS INT),
    CAST(NULLIF(cite_score_percentile, '-') AS INT),
    CAST(NULLIF(sjr_percentile, '-') AS INT)
FROM {{records}};
