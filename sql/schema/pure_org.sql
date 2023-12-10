CREATE TABLE csml_type_pure_org
(
    type_pure_org integer not null
        primary key,
    name_type_pure_org text
);
INSERT INTO csml_type_pure_org VALUES(1,'внутренние');
INSERT INTO csml_type_pure_org VALUES(2,'внешние');

CREATE TABLE IF NOT EXISTS csml_pure_org
(
	id_pure_org INTEGER not null primary key autoincrement,
	id_slice INTEGER not null,
	uuid TEXT not null,
	type_pure_org INTEGER not null
		references csml_type_pure_org,
	uuid_up TEXT,
	search_path TEXT,
	kind_pure_org TEXT,
	name_pure_org TEXT,
	level_org_unit INTEGER,
	name_pure_org_eng TEXT,
    -- temporary
    country_org TEXT,
    city_org TEXT
);

CREATE TABLE csml_type_pure_org_ids
(
    type_pure_org_ids integer not null
        primary key,
    name_type_pure_org_ids text
);
INSERT INTO csml_type_pure_org_ids VALUES(1,'BS');
INSERT INTO csml_type_pure_org_ids VALUES(2,'1C');
INSERT INTO csml_type_pure_org_ids VALUES(3,'Scopus aff_id');
INSERT INTO csml_type_pure_org_ids VALUES(4,'1c_new');
INSERT INTO csml_type_pure_org_ids VALUES(6,'rsci_id_org');

CREATE TABLE csml_pure_org_ids
(
    id_pure_org_ids   integer not null
            primary key autoincrement,
    id_pure_org       integer
            references csml_pure_org
            on delete cascade,
    type_pure_org_ids integer not null
        constraint csml_pure_org_ids_csml_type_pure_org_ids_type_pure_org_ids_fk
            references csml_type_pure_org_ids,
    num_pure_org_ids  text
);

CREATE TABLE csml_type_pure_org_slice
(
    type_pure_org_slice      integer not null
        primary key,
    name_type_pure_org_slice text
);
INSERT INTO csml_type_pure_org_slice VALUES(1,'по институтам');
INSERT INTO csml_type_pure_org_slice VALUES(2,'по департаментам');
INSERT INTO csml_type_pure_org_slice VALUES(3,'по внешним организациям');

CREATE TABLE csml_pure_org_slice
(
    id_pure_org_slice                   integer  not null
        primary key autoincrement,
    id_pure_org                         integer
            references csml_pure_org,
    type_pure_org_slice                 integer                                                                        not null
        references csml_type_pure_org_slice,
    name_org                            text,
    short_name_org                      text,
    id_slice                            integer,
    id_pure_org_subinstitute_transition integer,
    is_top_unit                         integer default 0                                                              not null
);

CREATE TABLE csml_pure_org_slice_norm
(
    id_pure_org_slice_norm      integer   not null
        primary key,
    type_pure_org_slice         integer                                                                                  not null,
    name_org                    text,
    short_name_org              text,
    type_update_pure_org_sclice integer default 1                                                                        not null
);
INSERT INTO csml_pure_org_slice_norm VALUES(1,1,'Институт Строительства и Архитектуры','ИСА',1);
INSERT INTO csml_pure_org_slice_norm VALUES(3,1,'Уральский гуманитарный институт','УрГИ',1);
INSERT INTO csml_pure_org_slice_norm VALUES(4,1,'Институт естественных наук и математики','ИЕНиМ',1);
INSERT INTO csml_pure_org_slice_norm VALUES(6,1,'Физико-технологический институт','ФТИ',1);
INSERT INTO csml_pure_org_slice_norm VALUES(7,1,'Уральский энергетический институт','УралЭНИН',1);
INSERT INTO csml_pure_org_slice_norm VALUES(8,1,'Уральский федеральный университет','УрФУ',0);
INSERT INTO csml_pure_org_slice_norm VALUES(9,1,'Институт экономики и управления','ИнЭУ',1);
INSERT INTO csml_pure_org_slice_norm VALUES(10,1,'Институт фундаментального образования','ИнФО',1);
INSERT INTO csml_pure_org_slice_norm VALUES(11,1,'Институт физической культуры, спорта и молодежной политики','ИФКСиМП',1);
INSERT INTO csml_pure_org_slice_norm VALUES(12,1,'Институт радиоэлектроники и информационных технологий - РтФ','ИРИТ-РтФ',1);
INSERT INTO csml_pure_org_slice_norm VALUES(13,1,'Химико-технологический институт','ХТИ',1);
INSERT INTO csml_pure_org_slice_norm VALUES(14,1,'Нижнетагильский технологический институт (филиал) УрФУ','НТИ',1);
INSERT INTO csml_pure_org_slice_norm VALUES(15,1,'Институт материаловедения и металлургии','ИНМТ',1);
INSERT INTO csml_pure_org_slice_norm VALUES(16,1,'Уральский федеральный университет не в институтах','УрфУother',-1);
INSERT INTO csml_pure_org_slice_norm VALUES(17,1,'Политехнический институт (филиал) в г. Каменске-Уральском','ПИ',1);

CREATE TABLE csml_pure_org_slice_norm_ids
(
    id_pure_org_slice_norm integer not null
        references csml_pure_org_slice_norm
            on delete cascade,
    uuid                   text    not null,
    zik_id_org             text,
    constraint csml_pure_org_slice_norm_ids_id_pure_org_slice_norm_zik_id_org_
        unique (id_pure_org_slice_norm, uuid)
);

INSERT INTO csml_pure_org_slice_norm_ids(id_pure_org_slice_norm, uuid, zik_id_org) VALUES
(1,'732a7cf9-da5a-49ae-ad39-3f8a479ac013','000000711'),
(3,'facc7fe3-eec8-4e0e-88e5-eb80e1462f0c','У00005052'),
(4,'7b987743-7bda-4815-a444-34bce4ff6a4b','000001134'),
(6,'2ce0fc8b-20b4-4dda-9708-3c5cc2220958','000000976'),
(7,'e89d0366-a651-493a-9022-4ec33e9245ab','000000975'),
(8,'9ab26a05-5638-442e-b7eb-db202a499370','000000007'),
(9,'409a5357-b76e-44d1-9cec-a8422c1cde5d',NULL),
(10,'c0044887-c71d-482a-9583-d4144ddf42af','000000850'),
(11,'65836e82-e3fc-4665-b2ec-a3ca5be9f813','000000803'),
(12,'2e07fe69-5991-4410-982a-b031b5fc2034','000000764'),
(13,'04cef6b6-b157-4dcf-9cc1-46af77773c2c','000000687'),
(14,'89ad250c-3507-4805-bc71-5c8d77ec2987','000000641'),
(15,'95e257fc-b03b-4d58-94c4-3298ae8e419a','У00004956'),
(17,'b8681a05-fafe-45b1-abc7-a837e0ceb3fb','000000642');

CREATE TABLE csml_pure_org_rel_slice
(
    id_pure_org       integer not null
        references csml_pure_org,
    id_pure_org_slice integer not null
        references csml_pure_org_slice,
    constraint csml_pure_org_rel_slice_id_pure_org_id_pure_org_slice_pk
        unique (id_pure_org, id_pure_org_slice)
);
