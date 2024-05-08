CREATE TABLE {{table}}(
    id_record_author INTEGER PRIMARY KEY,
    id_record INT REFERENCES {{records}},
    auid TEXT,
    seq_no INT,
    last_name TEXT,
    first_name TEXT,
    lang TEXT,
    affiliations TEXT,
    UNIQUE(id_record, seq_no)
);

INSERT INTO {{table}}(
    id_record,
    auid,
    seq_no,
    last_name,
    first_name,
    lang,
    affiliations
)
with partitioned as (
    select
        id_record,
        auid,
        seq_no,
        last_name,
        first_name,
        lang,
        affiliations,
        row_number() over (
            partition by id_record, seq_no
            order by (case
                when lang = 'RU' then 1
                when lang = 'EN' then 2
                else 3
            end)
        ) as rn
    from {{raw}}
)
select
    id_record,
    auid,
    seq_no,
    last_name,
    first_name,
    lang,
    affiliations
from partitioned
where rn = 1;
