INSERT INTO csml_record(id_record, id_slice, type_database_record, num_record, year_publ, cited_from_record,
                        lang_document, document_type, source_type, source_title, publisher,
                        source_country, doi, issn_norm, refs_count, keywords_count, authors_count)
SELECT
    id_record,
    {{slice}} as id_slice,
    3 as type_database_record,
    num_record,
    year_publ,
    cited_from_record,
    lang_document,
    document_type,
    source_type,
    source_title,
    publisher,
    source_country,
    doi,
    issn_norm,
    refs_count,
    keywords_count,
    authors_count
FROM tmp.csml_record;

INSERT INTO csml_record_category(id_record, type_category, value_category)
SELECT id_record, type_category, value_category
FROM tmp.csml_record_category;

INSERT INTO csml_record_author(id_record_author, id_record, auid, seq_no, last_name, first_name)
SELECT id_record_author, id_record, auid, seq_no, last_name, first_name
FROM tmp.csml_record_author;

INSERT INTO csml_record_affiliation(id_record_affiliation, id_record, afid, addr_no, full_address)
SELECT id_record_affiliation, id_record, afid, addr_no, full_address
FROM tmp.csml_record_affiliation;

INSERT INTO csml_record_author_rel_affiliation(id_record_author, id_record_affiliation)
SELECT id_record_author, id_record_affiliation
FROM tmp.csml_record_author_rel_affiliation
WHERE id_record_affiliation IS NOT NULL;
