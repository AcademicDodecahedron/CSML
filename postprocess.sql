/* классификация THE */
insert into csml_record_category(value_category,id_record,type_category)
select distinct csml_category_classification_code.code_category_classification_code, csml_record.id_record, 19
from csml_category_classification_code_mapping join csml_category_classification_code
   on csml_category_classification_code_mapping.id_category_classification_code = csml_category_classification_code.id_category_classification_code
and csml_category_classification_code_mapping.type_category_to=5
and csml_category_classification_code.type_category=19
    JOIN csml_record_category
ON  csml_category_classification_code_mapping.value_category=csml_record_category.value_category
AND csml_record_category.type_category=5
join csml_record
on csml_record.id_record= csml_record_category.id_record
and csml_record.id_slice = {{slice}};


/* классификация THE число категорий */
insert into csml_record_category_metrics (id_record,
        type_category,
        count_value_category)
        select csml_record.id_record, 19, count(*)
        from csml_record join csml_record_category crc on csml_record.id_record = crc.id_record
        and crc.type_category=19
        and id_slice = {{slice}}
        group by csml_record.id_record;


/* классификация QS */
insert into csml_record_category(value_category,id_record,type_category)
select distinct csml_category_classification_code.code_category_classification_code, csml_record.id_record, 6
from csml_category_classification_code_mapping join csml_category_classification_code
   on csml_category_classification_code_mapping.id_category_classification_code = csml_category_classification_code.id_category_classification_code
and csml_category_classification_code_mapping.type_category_to=5
and csml_category_classification_code.type_category=6
    JOIN csml_record_category
ON  csml_category_classification_code_mapping.value_category=csml_record_category.value_category
AND csml_record_category.type_category=5
join csml_record
on csml_record.id_record= csml_record_category.id_record
and csml_record.id_slice ={{slice}};

/* классификация QS число категорий */
insert into csml_record_category_metrics (id_record,
        type_category,
        count_value_category)
        select csml_record.id_record, 6, count(*)
        from csml_record join csml_record_category crc on csml_record.id_record = crc.id_record
        and crc.type_category=6
        and id_slice = {{slice}}
        group by csml_record.id_record;

/* старый способ считать число категорий для QS */
update csml_record
set QS_subj_count_rank=(
 select count(*) from csml_record_category
    where csml_record_category.id_record = csml_record.id_record
    and csml_record_category.type_category=6
)
where id_slice={{slice}};
