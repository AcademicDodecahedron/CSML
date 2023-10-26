create table csml_database_record
(
    id_database_record     integer not null
        primary key autoincrement,
    id_record              integer not null
        constraint csml_database_record_csml_record_id_record_fk
            references csml_record,
    name_database          text,
    num_record_in_database text
);
