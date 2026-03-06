# local/ — CSV Fixtures for Offline Testing

When running `python main.py --local`, DataSync Audit reads data from CSV files
in this folder instead of connecting to live databases.

## File naming

Each dataset configured in `config/master_datasets.xlsx` has two columns:
- `origin_query_file` — the origin SQL filename (e.g. `q_employees_origin.sql`)
- `destination_query_file` — the destination SQL filename

In **local mode**, those same filenames are used to look up CSVs here.
Simply name your fixture files to match — replacing `.sql` with `.csv`:

```
local/
  q_employees_origin.csv
  q_employees_destination.csv
  q_departments_origin.csv
  q_departments_destination.csv
  ...
```

## Column requirements

Each CSV must contain the columns referenced in the corresponding mapping file
(`config/mappings/<dataset>_mapping.xlsx`).  Column headers must match the
`Source_Column` values exactly for origin files, and `Target_Column` values
for destination files.

## .gitignore note

The `.gitignore` excludes `local/*.csv` by default to prevent accidental
commit of real customer data.  Add fixture files manually if you need them
in version control (use anonymised / synthetic data only).
