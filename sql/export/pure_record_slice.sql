{% macro id_slice_urfu() -%}
(select id_pure_org_slice from csml_pure_org_slice where is_top_unit=1)
{%- endmacro %}

{% macro id_slice_urfu_other() -%}
(select id_pure_org_slice from csml_pure_org_slice where is_top_unit=-1)
{%- endmacro %}

-- all
insert into csml_record_pure_org_slice(id_pure_org_slice, id_record, type_record_org_slice)
select distinct csml_pure_org_slice.id_pure_org_slice, csml_record.id_record, 1
from csml_record
         join csml_record_affiliation
              on csml_record.id_record = csml_record_affiliation.id_record
                  and csml_record_affiliation.full_address = 'SubUrFU'
         join csml_pure_org_slice on csml_pure_org_slice.id_pure_org = csml_record_affiliation.id_pure_org
    and csml_pure_org_slice.id_slice = csml_record.id_slice
where csml_pure_org_slice.id_pure_org_slice not in ({{id_slice_urfu() | sql}}, {{id_slice_urfu_other() | sql}});

-- other
insert into csml_record_pure_org_slice(id_pure_org_slice, id_record, type_record_org_slice)
select {{id_slice_urfu_other() | sql}}, id_record, 1
from csml_record
where not exists(select 1
                 from csml_pure_org_slice
                          join csml_record_pure_org_slice on
                             csml_pure_org_slice.id_pure_org_slice = csml_record_pure_org_slice.id_pure_org_slice
                         and csml_record_pure_org_slice.id_record = csml_record.id_record
                         and csml_pure_org_slice.id_slice = csml_record.id_slice);

-- UrFU
insert into csml_record_pure_org_slice(id_pure_org_slice, id_record, type_record_org_slice)
select {{id_slice_urfu() | sql}}, id_record, 1
from csml_record;
