create table csml_type_database_record
(
    type_database_record      integer not null
        primary key,
    name_type_database_record text
);


INSERT INTO csml_type_database_record (type_database_record, name_type_database_record) VALUES (1, 'WoS');
INSERT INTO csml_type_database_record (type_database_record, name_type_database_record) VALUES (2, 'Scopus');
INSERT INTO csml_type_database_record (type_database_record, name_type_database_record) VALUES (3, 'РИНЦ');
INSERT INTO csml_type_database_record (type_database_record, name_type_database_record) VALUES (4, 'Pure');

create table csml_type_state_load
(
    type_state_load      integer not null
        primary key,
    name_type_state_load text
);


INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (0, 'запись создана');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (1, 'загружена базовая информация о записи (журнал, категории, выходные данные)');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (2, 'загружены авторы');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (3, 'обновлено цитирование');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (4, 'требуется обновление цитирования');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (5, 'троебуется загрузка записи из Scopus');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (6, 'загрузили записи из Scopus');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (7, 'отложена загрузка из Scopus');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (8, 'требуется обновление цитирования для новой загруженной записи');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (9, 'записи из Scopus загружены полностью');
INSERT INTO csml_type_state_load (type_state_load, name_type_state_load) VALUES (10, 'запись WoS из RIS');


create table csml_record
(
    id_record                                 INTEGER not null
        primary key autoincrement,
    id_slice                                  integer           not null,
    num_record                                varchar(50)       not null,
    type_database_record                      integer           not null
        constraint csml_record_csml_type_database_record_type_database_record_fk
            references csml_type_database_record,
    year_publ                                 text,
    cited_from_record                         integer,
    cited_verified                            integer,
    cited_verified_without_selfciting         integer,
    lang_document                             text,
    document_type                             text,
    source_type                               text,
    id_source                                 integer
        constraint csml_record_csml_source_id_source_fk
            references csml_source,
    source_title                              text,
    publisher                                 text,
    source_country                            text,
    cover_sort_date                           date,
    doi                                       text,
    issn_norm                                 text,
    hlp_short_name_org_for_counting           text,
    refs_count                                integer,
    fund_text                                 text,
    delivered_date                            date,
    type_state_load                           integer default 0 not null
        constraint csml_record_csml_type_state_load_type_state_load_fk
            references csml_type_state_load,
    scopus_sourceid                           text,
    qs_subj_count_rank                        integer,
    elibrary_sourceid                         text,
    cited_verified_without_selfciting_authors integer default 0 not null,
    keywords_count                            integer,
    authors_count                             integer,
    oa                                        text,
    international                             text,
    collaboration                             text,
    title_record                              text
);


create index csml_record_id_slice_num_record_index
    on csml_record (id_slice, num_record);

create index csml_record_id_slice_type_state_load_index
    on csml_record (id_slice, type_state_load);

create index csml_record_id_slice_year_publ_source_type_index
    on csml_record (id_slice, year_publ, source_type);



create table csml_type_category
(
    type_category          integer not null
        primary key,
    name_type_category     text,
    type_category_database text
);

INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (1, 'heading', 'WoS');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (2, 'subheading', 'WoS');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (5, 'ASJC', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (6, 'QSSubject', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (7, 'OECD', 'WoS/Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (8, 'VAK', 'Pure');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (9, 'AF-ID записей', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (10, 'GRNTI', 'ELibrary');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (11, 'quality record eLibrary', 'ELibrary');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (12, 'Competencies', 'SciVal');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (13, 'UFMA_TR', 'WoS');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (14, 'traditional_string_agg', 'WoS');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (15, 'scimagojr.com', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (16, 'WoS database', 'WoS');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (17, 'ASCJ_string_agg', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (18, 'Essential Science Indicators', 'WoS');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (4, 'extended', 'WoS / Research Areas');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (3, 'traditional', 'WoS / Web of Science Categories ');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (19, 'THESubject', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (20, 'ShortNameOrg', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (21, 'ASJC_description_string_agg', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (22, 'country_string_agg', 'Scopus / SciVal');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (23, 'country', 'Scopus / SciVal');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (24, 'Scopusaffiliationnames', 'Scopus / SciVal');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (25, 'Institutions SciVal', 'SciVal');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (26, 'SNIP ASJC', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (31, 'CiteScore ASJC', 'Scopus');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (32, 'SNTR', 'СНТР');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (33, 'ARWU Subject 2020', 'WoS');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (34, 'QSSubject from SciVal', 'SciVal');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (35, 'THESubject from SciVal', 'SciVal');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (36, 'SustainableDevelopmentGoals', 'SciVal');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (28, 'Кафедра', 'Local Org System');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (29, 'Институт/Факультет#Кафедра', 'Local Org System');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (27, 'Институт/Факультет', 'Local Org System ');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (30, 'Парк или Университет', 'Local Org System');
INSERT INTO csml_type_category (type_category, name_type_category, type_category_database) VALUES (37, 'Source File', 'MultiFiles Load');


create table csml_record_category
(
    id_record_category INTEGER not null
        primary key autoincrement,
    id_record          integer                                                                          not null
        constraint csml_record_category_record_fk
            references csml_record,
    type_category      integer                                                                          not null
        constraint csml_record_category_type_category_fk
            references csml_type_category,
    value_category     text                                                                             not null
);


create index csml_record_category_type_category_id_record_index
    on csml_record_category (type_category, id_record);


create table csml_type_record_ids
(
    type_record_ids      integer not null
        primary key,
    name_type_record_ids text
);


INSERT INTO csml_type_record_ids (type_record_ids, name_type_record_ids) VALUES (1, 'eid Scopus');
INSERT INTO csml_type_record_ids (type_record_ids, name_type_record_ids) VALUES (2, 'SGR Scopus');
INSERT INTO csml_type_record_ids (type_record_ids, name_type_record_ids) VALUES (3, 'WoS');
INSERT INTO csml_type_record_ids (type_record_ids, name_type_record_ids) VALUES (4, 'researchoutputwizard');
INSERT INTO csml_type_record_ids (type_record_ids, name_type_record_ids) VALUES (5, 'РИНЦ');
INSERT INTO csml_type_record_ids (type_record_ids, name_type_record_ids) VALUES (6, 'PubMed');
INSERT INTO csml_type_record_ids (type_record_ids, name_type_record_ids) VALUES (7, 'crossref');


create table csml_record_ids
(
    id_record_ids   INTEGER not null
        primary key autoincrement,
    type_record_ids integer                                                                not null
        constraint csml_record_ids_csml_type_record_ids_type_record_ids_fk
            references csml_type_record_ids,
    num_record_ids  text,
    id_record       integer                                                                not null
        constraint csml_record_ids_csml_record_id_record_fk
            references csml_record
);


create index csml_record_ids_id_record_type_record_ids_index
    on csml_record_ids (id_record, type_record_ids);

create index csml_record_ids_type_record_ids_num_record_ids_index
    on csml_record_ids (type_record_ids, num_record_ids);

create table csml_record_relation
(
    id_record     integer not null
        constraint csml_record_relation_csml_record_id_record_fk
            references csml_record,
    id_slice_to   integer not null,
    id_record_rel integer not null
        constraint csml_record_relation_csml_record_id_record_rel_fk
            references csml_record,
    id_slice_from integer not null
);

create unique index csml_record_relation_id_slice_from_id_record_id_slice_to_id_rec
    on csml_record_relation (id_slice_from, id_record, id_slice_to, id_record_rel);

create table csml_record_author
(
    id_record_author       INTEGER not null
        primary key autoincrement,
    id_record              integer not null
        constraint csml_contributor_record_fk
            references csml_record,
    display_name           text,
    full_name              text,
    wos_standard           text,
    first_name             text,
    last_name              text,
    email_addr             text,
    dais_id                text,
    reprint                varchar(10),
    daisng_id              text,
    role                   text,
    seq_no                 integer,
    auid                   text,
    initials               text,
    suffix                 text,
    degrees                text,
    preferred_initials     text,
    preferred_indexed_name text,
    preferred_surname      text,
    preferred_givenname    text,
    id_pure_person         integer
);

create table csml_record_affiliation
(
    id_record_affiliation INTEGER not null
        primary key autoincrement,
    id_record             integer                                                                                not null
        constraint csml_record_affiliation_record_fk
            references csml_record,
    addr_no               integer,
    full_address          text,
    city                  text,
    country               text,
    zip                   text,
    ziplocation           text,
    affiliation_city      text,
    affiliation_country   text,
    afid                  text,
    state                 text,
    city_group            text,
    country_code          text,
    id_pure_org           integer
        constraint csml_record_affiliation_csml_pure_org_id_pure_org_fk,
    only_record_level     integer default 0,
    reprint               varchar(10)
);


create index csml_record_affiliation_afid_index
    on csml_record_affiliation (afid);

create index csml_record_affiliation_id_pure_org_index
    on csml_record_affiliation (id_pure_org);

create index csml_record_affiliation_id_record_addr_no_index
    on csml_record_affiliation (id_record, addr_no);

create index csml_record_affiliation_id_record_afid_index
    on csml_record_affiliation (id_record, afid);


create table csml_record_author_rel_affiliation
(
    id_record_author      integer not null
        constraint csml_record_author_rel_affiliation_csml_record_author_id_record
            references csml_record_author,
    id_record_affiliation integer not null
        constraint csml_record_author_rel_affiliation_csml_record_affiliation_id_r
            references csml_record_affiliation,
    order_aff             integer,
    reprint               varchar(10),
    constraint csml_record_author_rel_affiliation_id_record_author_id_record_a
        unique (id_record_author, id_record_affiliation)
);

create table csml_pure_project
(
    id_project         INTEGER not null
        primary key autoincrement,
    id_slice           integer                                                          not null,
    num_source         text,
    typeclassification text,
    title              text
);

create index csml_project_id_slice_num_source_index
    on csml_pure_project (id_slice, num_source);


create table csml_type_record_org_slice
(
    type_record_org_slice      integer not null
        primary key,
    name_type_record_org_slice text
);



INSERT INTO csml_type_record_org_slice (type_record_org_slice, name_type_record_org_slice) VALUES (1, 'по институту в записи');
INSERT INTO csml_type_record_org_slice (type_record_org_slice, name_type_record_org_slice) VALUES (2, 'по институту автора');


create table csml_record_pure_org_slice
(
    id_pure_org_slice     integer not null
        references csml_pure_org_slice,
    id_record             integer not null
        references csml_record,
    type_record_org_slice integer default 1
        constraint csml_record_pure_org_slice_csml_type_record_org_slice_type_reco
            references csml_type_record_org_slice,
    constraint csml_record_pure_org_slice_id_pure_org_slice_type_record_org_sl
        unique (id_pure_org_slice, type_record_org_slice, id_record)
);

create table csml_record_project
(
    id_record  INT not null
        references csml_record,
    id_project INT not null
        references csml_pure_project
);
