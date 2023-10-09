create table csml_record_category_metrics
(
    id_record            integer not null
        constraint csml_record_category_record_fk
            references csml_record,
    type_category        integer not null
        constraint csml_record_category_type_category_fk
            references csml_type_category,
    count_value_category integer not null,
    constraint csml_record_category_metrics_pk
        primary key (id_record, type_category)
);

