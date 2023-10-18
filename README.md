# CSML Cube Importer

## `csml.py`

```
usage: csml.py [-h] -c CONFIG [-s SLICE] [--continue] output
```

Required arguments:

- `-c CONFIG`, `--config CONFIG` - YAML configuration file that describes source files, csml schema, etc.
- `output` - where to write the sqlite database

Optional arguments:

- `-s SLICE`, `--slice SLICE` - `id_slice` value, 0 by default
- `--continue` - with this flag, the temporary database (not to be confused with the `output` database)  
will not be deleted and the pipeline will continue where it left off.  
  Useful if you want to debug the *schema* or *export* scripts without re-generating the temporary database.

## Config file

Import configuration needs to be described in a `.yml` file with the following structure.


### `source`
Source-specific configurations, click to open:

<details>
<summary><b>scival</b></summary>

[Example config](./scival.yml)

- **`path`** - a SciVal CSV file or a folder containing CSVs
- **`header_length`** - number of lines to skip before the CSV data block.  
If set to `auto`, assume that the header ends after 2 or more consecutive empty lines.
- **`fields`** - a mapping of `"Field Name in CSV": field_name_in_sqlite`,  
  where `field_name_in_sqlite` will be used to dynamically generate the table `tmp.records`.  
  Fields not present in this mapping will not be imported.

  The specific fields that are necessary depend on the export scripts you're using.
  <details>
  <summary>List absolutely required fields</summary>

    - eid
    - scopus_source_title
    - issn
    - num_source
    - source_type
    - scopus_author_ids
    - scopus_affiliation_ids
    - topic_number
    - topic_name
    - topic_prominence_percentile
    - topic_cluster_number
    - topic_cluster_name
    - topic_cluster_prominence_percentile
    - field_weighted_view_impact
    - field_weighted_citation_impact
    - views
    - outputs_in_top_citation_percentiles_per_percentile
    - field_weighted_outputs_in_top_citation_percentiles_per_percentile
    - snip
    - cite_score
    - sjr
    - number_of_authors
    - snip_percentile
    - cite_score_percentile
    - sjr_percentile
  </details>
- **`category_mapping`** - a mapping of `"field_name_in_sqlite": type_category`,  
  where `type_category` is an integer referencing `csml_type_category.type_category`.  

  Each of the fields listed will be split by the separator `|` or `,` and inserted into `csml_type_category`

#### `check_scival_fields.py`
Often times you'll want to update an existing mapping for a new set of CSVs.  
You can use this utility to list which fields are missing in the new CSV,
as well as what other fields are availiable.

```
usage: check_scival_fields.py [-h] [-c CONFIG] csv header_length
```

If `csv` is a folder, will additionally check whether all its CSV files have the same columns

Required arguments:
- `csv` - a SciVal CSV file or a folder containing CSVs
- `header_length` - integer or `auto`

Optional arguments:
- `-c CONFIG`, `--config CONFIG` - scival config to compare the CSV against
</details>

### `schema`
List of sql scripts that create an empty CSML schema:

```yml
schema:
  - ./sql/schema/csml_source.sql
  - ./sql/schema/pure_record.sql
  - ...
```

### `export`
List of sql scripts that copy data from the temporary database (named `tmp`)
into the CSML schema.

Receives a jinja variable `{{slice}}` that resolves to the `--slice` cli argument

```yml
export:
  - ./sql/export/export_scival.sql
  - ./sql/export/postprocess_scival.sql
```
