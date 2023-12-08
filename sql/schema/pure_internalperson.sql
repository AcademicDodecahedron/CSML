create table csml_type_pure_person
(
    type_pure_person      integer not null
        primary key,
    name_type_pure_person text
);

INSERT INTO csml_type_pure_person (type_pure_person, name_type_pure_person) VALUES (1, 'внутренная');
INSERT INTO csml_type_pure_person (type_pure_person, name_type_pure_person) VALUES (2, 'внешная');


create table csml_type_pure_person_name
(
    type_pure_person_name      integer not null
        primary key,
    name_type_pure_person_name text
);


INSERT INTO csml_type_pure_person_name (type_pure_person_name, name_type_pure_person_name) VALUES (1, 'неизвестно');
INSERT INTO csml_type_pure_person_name (type_pure_person_name, name_type_pure_person_name) VALUES (2, 'nameVariants');
INSERT INTO csml_type_pure_person_name (type_pure_person_name, name_type_pure_person_name) VALUES (3, 'former');
INSERT INTO csml_type_pure_person_name (type_pure_person_name, name_type_pure_person_name) VALUES (4, 'defaultpublishing');
INSERT INTO csml_type_pure_person_name (type_pure_person_name, name_type_pure_person_name) VALUES (5, 'translated');
INSERT INTO csml_type_pure_person_name (type_pure_person_name, name_type_pure_person_name) VALUES (6, 'report_short_name');
INSERT INTO csml_type_pure_person_name (type_pure_person_name, name_type_pure_person_name) VALUES (0, 'нормативное имя в кадрах');

create table csml_type_pure_person_id
(
    type_pure_person_id      integer not null
        primary key,
    name_type_pure_person_id text,
    mark_type_pure_person_id text
);


INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (1, '1С', 'synchronisedUnifiedPerson');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (2, 'scopusauthor', 'scopusauthor');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (3, 'WOS_DAIS', 'WOS_DAIS');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (4, 'Scopus', 'Scopus');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (5, 'employee', 'employee');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (6, 'researcher', 'researcher');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (7, 'rsci', 'rsci');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (8, 'sciencemapRFauthor', 'sciencemapRFauthor');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (9, 'orcid', 'orcid');
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (0, 'неизвестно', null);
INSERT INTO csml_type_pure_person_id (type_pure_person_id, name_type_pure_person_id, mark_type_pure_person_id) VALUES (10, 'employee_old', 'employee_old');


create table csml_pure_person
(
    id_pure_person             INTEGER not null
        primary key autoincrement,
    type_pure_person           integer                                                                  not null
        constraint csml_pure_person_csml_type_pure_person_type_pure_person_fk
            references csml_type_pure_person,
    uuid                       text                                                                     not null,
    firstname                  text,
    lastname                   text,
    jobdescription             text,
    personeducation            text,
    year_get_education         integer,
    professionalqualifications text,
    id_pure_org                integer
        constraint csml_pure_person_csml_pure_org_id_pure_org_fk
            references csml_pure_org,
    id_slice                   integer                                                                  not null,
    association                text,
    id_pure_org_topunit        integer,
    id_pure_org_subtopunit     integer,
    year_of_birth              integer,
    international              integer
);

create table csml_pure_person_id
(
    id_pure_person_id   INTEGER not null
        primary key autoincrement,
    type_pure_person_id integer,
    value_id            text,
    id_pure_person      integer
        constraint csml_pure_person_id_csml_pure_person_id_pure_person_fk
            references csml_pure_person
);



create index csml_pure_person_id_id_pure_person_type_pure_person_id_index
    on csml_pure_person_id (id_pure_person, type_pure_person_id);

create index csml_pure_person_id_type_pure_person_id_value_id_index
    on csml_pure_person_id (type_pure_person_id, value_id);


create table csml_pure_person_name_variant
(
    id_pure_person_name_variant INTEGER not null
        primary key autoincrement,
    id_pure_person              integer                                                                                            not null
        constraint csml_pure_person_name_variant_csml_pure_person_id_pure_person_f
            references csml_pure_person,
    type_pure_person_name       integer                                                                                            not null,
    firstname                   text,
    lastname                    text
);


create index csml_pure_person_name_variant_type_pure_person_name_lastname_fi
    on csml_pure_person_name_variant (type_pure_person_name, lastname, firstname);


create table csml_pure_person_rel_org
(
    id_pure_person_rel_org INTEGER not null
        primary key autoincrement,
    id_pure_person         integer                                                                                  not null
        constraint csml_pure_person_rel_org_csml_pure_person_id_pure_person_fk
            references csml_pure_person,
    id_pure_org            integer                                                                                  not null
        constraint csml_pure_person_rel_org_csml_pure_org_id_pure_org_fk
            references csml_pure_org,
    type_work              text,
    startdate              date,
    enddate                date,
    primaryassociation     integer,
    jobdescription         text,
    jobname                text
);

