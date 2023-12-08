insert into csml_pure_org
select org_id,
       {{slice}}      as id_slice,
       uuid,
       type_pure_org,
       parent_uuid as uuid_up,
       search_path,
       kind_pure_org,
       name_pure_org,
       level       as level_org_unit,
       name_pure_org_eng,
       country_org,
       city_org
from tmp.orgs;

insert into csml_pure_org_ids(id_pure_org, type_pure_org_ids, num_pure_org_ids)
select org_id   as id_pure_org,
       type_pure_org_ids,
       id_value as num_pure_org_ids
from tmp.org_ids
where type_pure_org_ids is not null;

insert into csml_pure_person(id_pure_person, id_slice, type_pure_person, uuid, firstname, lastname, year_of_birth,
                             personeducation, professionalqualifications)
select id_person,
       {{slice}} as id_slice,
       type_pure_person,
       uuid,
       first_name,
       last_name,
       year_of_birth,
       education,
       qualification
from tmp.person;

insert into csml_pure_person_id(type_pure_person_id, value_id, id_pure_person)
select type_id,
       id_value,
       id_person
from tmp.person_ids
where type_id is not null;

insert into csml_pure_person_name_variant (id_pure_person, type_pure_person_name, firstname, lastname)
select id_person,
       type_id,
       firstname,
       lastname
from tmp.person_name_variants
where type_id is not null;

insert into csml_pure_person_rel_org (id_pure_person, id_pure_org, type_work, startdate, enddate, primaryassociation,
                                      jobdescription, jobname)
select id_person,
       org_id,
       type_work,
       start_date,
       end_date,
       is_primary_asociation,
       job_description,
       job_name
from tmp.person_associations;

insert into csml_record(id_record, id_slice, type_database_record, num_record, doi, year_publ, source_type,
                        source_title, document_type, issn_norm, scopus_sourceid)
select id_record,
       {{slice}}        as id_slice,
       4             as type_database_record,
       record_uuid,
       doi,
       year_publication,
       node_name     as source_type,
       journal_title as source_title,
       output_type   as document_type,
       issn,
       journal_uuid
from tmp.record
where record_uuid is not null;

insert into csml_record_category (id_record, type_category, value_category)
select id_record,
       type_category,
       value_category
from tmp.record_categories;

insert into csml_record_ids (id_record, type_record_ids, num_record_ids)
select id_record,
       id_source,
       id_value
from tmp.record_ids
where id_source is not null;

INSERT INTO csml_record_author (id_record_author, id_record, display_name, first_name, last_name, role,
                                id_pure_person)
select id_record_author,
       id_record,
       full_name,
       first_name,
       last_name,
       role,
       id_person
from tmp.record_authors;

INSERT INTO csml_record_affiliation (id_record_affiliation, id_record, id_pure_org, full_address,
                                     only_record_level)
select id_record_affiliation,
       id_record,
       org_id,
       full_address,
       only_record_level
from tmp.record_affiliations;

INSERT INTO csml_record_author_rel_affiliation (id_record_author, id_record_affiliation, order_aff)
select id_record_author,
       id_record_affiliation,
       0 as order_aff
from tmp.record_author_rel_affiliations;

insert into csml_pure_project (id_project, id_slice, num_source, typeclassification, title)
select id_project,
       {{slice}} as id_slice,
       uuid,
       type_classification,
       title
from tmp.projects;

INSERT INTO csml_record_project (id_record, id_project)
select id_record,
       id_project
from tmp.record_projects;
