-- вставка нового среза по топовым организациям csml_pure_org_slice 
insert into csml_pure_org_slice (id_pure_org, type_pure_org_slice, name_org, short_name_org, id_slice)
select csml_pure_org.id_pure_org,
       csml_pure_org_slice_norm.type_pure_org_slice,
       csml_pure_org_slice_norm.name_org,
       csml_pure_org_slice_norm.short_name_org,
       csml_pure_org.id_slice
from csml_pure_org
         join csml_pure_org_slice_norm_ids
              on csml_pure_org.uuid = csml_pure_org_slice_norm_ids.uuid
                  and csml_pure_org.id_slice = {{slice}}
         join csml_pure_org_slice_norm
              on csml_pure_org_slice_norm.id_pure_org_slice_norm = csml_pure_org_slice_norm_ids.id_pure_org_slice_norm
                  and csml_pure_org_slice_norm.type_update_pure_org_sclice = 1;

-- собираем указатель какие подразедения в каие топовые юниты объединять. Без УрФу
insert into csml_pure_org_rel_slice (id_pure_org_slice, id_pure_org)
select DISTINCT csml_pure_org_slice.id_pure_org_slice, o2.id_pure_org
from csml_pure_org_slice
         join csml_pure_org o1 on o1.id_pure_org = csml_pure_org_slice.id_pure_org
    and csml_pure_org_slice.type_pure_org_slice = 1
    and o1.id_slice = {{slice}}
    and o1.id_slice = csml_pure_org_slice.id_slice
         join csml_pure_org o2 on o2.search_path like o1.search_path || '%';

-- это УрФУ
insert into csml_pure_org_slice (id_pure_org, type_pure_org_slice, name_org, short_name_org, id_slice, is_top_unit)
select csml_pure_org.id_pure_org,
       csml_pure_org_slice_norm.type_pure_org_slice,
       csml_pure_org_slice_norm.name_org,
       csml_pure_org_slice_norm.short_name_org,
       csml_pure_org.id_slice,
       1
from csml_pure_org
         join csml_pure_org_slice_norm_ids
              on csml_pure_org.uuid = csml_pure_org_slice_norm_ids.uuid
                  and csml_pure_org.id_slice = {{slice}}
         join csml_pure_org_slice_norm
              on csml_pure_org_slice_norm.id_pure_org_slice_norm = csml_pure_org_slice_norm_ids.id_pure_org_slice_norm
                  and csml_pure_org_slice_norm.type_update_pure_org_sclice = 0;

-- это подразделения не в институтах УрФУ
insert into csml_pure_org_slice (id_pure_org, type_pure_org_slice, name_org, short_name_org, id_slice, is_top_unit)
select NULL,
       csml_pure_org_slice_norm.type_pure_org_slice,
       csml_pure_org_slice_norm.name_org,
       csml_pure_org_slice_norm.short_name_org,
       {{slice}},
       -1
from csml_pure_org_slice_norm
where csml_pure_org_slice_norm.type_update_pure_org_sclice = -1;
