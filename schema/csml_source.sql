create table csml_source
(
    id_source                   serial
        primary key,
    id_slice                    integer           not null
        constraint csml_source_csml_slice_id_slice_fk
            references csml_slice,
    num_source                  text,
    source_title                text,
    source_abbrev               text,
    source_type                 text,
    publisher                   text,
    source_country              text,
    issn_norm                   text,
    website                     text,
    quartile_sjr_current        integer,
    sjr_current                 numeric(10, 3),
    quartile_snip_current       integer,
    snip_current                numeric(10, 3),
    quartile_if_current         integer,
    if_current                  numeric(10, 3),
    openaccess                  text,
    openaccessarticle           text,
    openarchivearticle          text,
    openaccesstype              text,
    openaccessstartdate         text,
    oaallowsauthorpaid          text,
    publishermain               text,
    coverage                    text,
    activeorinactive            text,
    citescore_current           numeric(10, 3),
    medline                     text,
    articlesinpress             text,
    added_last_edition          text,
    language                    text,
    sjr_count_category_quartile integer default 0 not null,
    pubcode                     text,
    url                         text,
    region                      text,
    source_title_upper          text,
    count_qs                    integer,
    count_the                   integer,
    count_asjc                  integer,
    type_source                 text,
    rus_list                    text,
    id_record_example           integer
);

create index csml_source_id_slice_num_source_index
    on csml_source (id_slice, num_source);
