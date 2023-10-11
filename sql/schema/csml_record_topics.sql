create table csml_record_topics
(
    id_record            integer           not null,
    num_topics           text              not null,
    name_topics          text              not null,
    iscluster            integer default 0 not null,
    prominencepercentile numeric(6, 3)
);

create index csml_record_topics_id_record_iscluster_index
    on csml_record_topics (id_record, iscluster);


