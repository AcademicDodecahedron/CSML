INSERT INTO csml_record(
    id_record,
    id_slice,
    num_record,
    type_database_record,
    doi,
    document_type,
    source_title,
    source_type,
    lang_document,
    refs_count,
    cited_from_record,
    issn_norm,
    year_publ
)
SELECT
    id_record,
    {{ slice }},
    num_record,
    1 AS type_database_record, -- WoS
    doi,
    document_type,
    source_title,
    source_type,
    lang_document,
    refs_count,
    cited_from_record,
    issn,
    year_publ
FROM tmp.records;

INSERT INTO csml_record_topics(
    id_record,
    num_topics,
    name_topics,
    iscluster
)
SELECT
    id_record,
    num_topics,
    name_topics,
    iscluster
FROM tmp.record_topics;

INSERT INTO csml_record_metrics(
    id_record,
    incites_times_cited,
    incites_journal_expected_citations,
    incites_category_expected_citations,
    incites_journal_normalized_citation_impact,
    incites_category_normalized_citation_impact,
    incites_percentile_in_subject_area,
    incites_journal_impact_factor
)
SELECT
    id_record,
    incites_times_cited,
    incites_journal_expected_citations,
    incites_category_expected_citations,
    incites_journal_normalized_citation_impact,
    incites_category_normalized_citation_impact,
    incites_percentile_in_subject_area,
    incites_journal_impact_factor
FROM tmp.record_metrics;

INSERT INTO csml_record_author(
    id_record_author,
    id_record,
    full_name,
    first_name,
    last_name
)
SELECT
    id_record_author,
    id_record,
    full_name,
    first_name,
    last_name
FROM tmp.record_authors;

INSERT INTO csml_record_affiliation(
    id_record_affiliation,
    id_record,
    full_address,
    country,
    city,
    zip
)
SELECT
    id_record_affiliation,
    id_record,
    full_address,
    country,
    city,
    "index"
FROM tmp.record_affiliations;

INSERT INTO csml_record_author_rel_affiliation(
    id_record_author,
    id_record_affiliation,
    order_aff
)
SELECT
    id_record_author,
    id_record_affiliation,
    "order"
FROM tmp.rel_affiliations;

INSERT INTO csml_record_category(
    id_record,
    type_category,
    value_category
)
SELECT
    id_record,
    type_category,
    value_category
FROM tmp.record_category;

INSERT INTO csml_database_record(
    id_record,
    name_database,
    num_record_in_database
)
SELECT
    id_record,
    name_database,
    num_record_in_database
FROM tmp.database_record;
