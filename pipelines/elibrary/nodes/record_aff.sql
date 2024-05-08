CREATE TABLE {{table}}(
    id_record_affiliation INTEGER PRIMARY KEY,
    id_record INT REFERENCES {{records}},
    afid TEXT,
    addr_no INT,
    full_address TEXT,
    lang TEXT,
    UNIQUE (id_record, addr_no)
);

INSERT INTO {{table}}(id_record, afid, addr_no, full_address, lang)
WITH partitioned as 
(
    SELECT id_record, afid, addr_no, full_address, lang,
    row_number() over (
        partition by id_record, addr_no
        order by case
            when lang = 'RU' then 1
            when lang = 'EN' then 2
            else 3
        end
    ) as rn
    FROM {{rel_aff}}
) SELECT id_record, afid, addr_no, full_address, lang
FROM partitioned WHERE rn = 1;
