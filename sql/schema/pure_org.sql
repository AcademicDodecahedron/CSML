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
