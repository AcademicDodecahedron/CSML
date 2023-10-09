insert into csml_record (id_record, id_slice, file_name, type_database_record, num_record, cited_from_record, year_publ,
                         type_state_load,
                         id_source, scopus_sourceid, doi, source_type, source_title, document_type, authors_count,
                         issn_norm)
select id_record,
       {{slice}} as id_slice,
       filename,
       2 as type_database_record,
       sgr,
       citations,
       year,
       0 as type_state_load,
       id_source,
       num_source,
       doi,
       source_type,
       scopus_source_title,
       publication_type,
       number_of_authors,
       issn
from tmp.records;

insert into csml_record_affiliation (id_record_affiliation, id_record, afid)
select id_record_affiliation,
       id_record,
       afid
from tmp.record_affiliation;

insert into csml_record_author (id_record_author, id_record, auid)
select id_record_author,
       id_record,
       auid
from tmp.record_authors;

insert into csml_record_category (file_name, id_record, type_category, value_category)
select filename, id_record, type_category, value_category
from tmp.record_category
where value_category is not null;

INSERT INTO csml_record_ids (type_record_ids, num_record_ids, id_record)
select type_record_ids,
       num_record_ids,
       id_record
from tmp.record_ids;

insert into csml_record_metrics(id_record, fieldweightedviewimpact, fieldweightedcitationimpact, views,
                                outputsintopcitationpercentilesperpercentile,
                                fieldweightedoutputsintopcitationpercentilesperpercentile, snip_current, sjr_current,
                                citescore_current, numberofauthors, SNIPpercentile, CiteScorepercentile, SJRpercentile)
select id_record,
       field_weighted_view_impact,
       field_weighted_citation_impact,
       views,
       outputs_in_top_citation_percentiles_per_percentile,
       field_weighted_outputs_in_top_citation_percentiles_per_percentile,
       snip,
       cite_score,
       sjr,
       number_of_authors,
       snip_percentile,
       cite_score_percentile,
       sjr_percentile
from tmp.record_metrics;

insert into csml_record_topics (id_record, num_topics, name_topics, iscluster, ProminencePercentile)
select id_record,
       num_topics,
       name_topics,
       iscluster,
       ProminencePercentile
from tmp.record_topics;

-- insert into csml_record_author_rel_affiliation(id_record_author, id_record_affiliation)
-- select id_record_author,
--        id_record_affiliation
-- from tmp.rel_aff_joined;
