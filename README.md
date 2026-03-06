# DataSync Audit

**Cross-System Data Reconciliation Framework**

DataSync Audit is a Python framework for validating data integrity between two heterogeneous database systems. It extracts data from an origin and a destination system, applies configurable schema mappings and transformation rules, and produces detailed row-level audit reports that identify exactly where data matches, mismatches, or is missing.

Typical use cases include post-migration validation, ETL pipeline auditing, and ongoing cross-system data consistency checks.

---

## Features

- **Multi-connector** — Supports PostgreSQL and SQL Server / Azure SQL out of the box.
- **Declarative mappings** — Column renames, type coercions, ID translations, and prefix-stripping are all defined in Excel mapping files — no code changes required.
- **Row-level reconciliation** — Every row is classified as `MATCH`, `MISMATCH`, `MISSING_IN_DEST`, or `EXTRA_IN_DEST`.
- **Audit trail** — Original origin values are preserved alongside transformed values in reports, enabling full traceability.
- **Rich reports** — Each run produces a summary CSV, a combined CSV, and a colour-coded HTML report per dataset.
- **Azure DevOps integration** — Results can be published to an ADO Test Run in real time as each dataset completes.
- **Local fixture mode** — Run against CSV files for development and CI testing without live database connections.

---

## Project Structure

```
datasync_audit/
├── main.py                         # CLI entry point & pipeline orchestrator
│
├── core/                           # Core processing components
│   ├── schema_parser.py            # Excel mapping file parser
│   ├── rule_engine.py              # Transformation rule implementations
│   └── transform_engine.py        # Applies rules to raw DataFrames
│
├── connectors/                     # Database extraction connectors
│   ├── postgres_connector.py       # PostgreSQL via psycopg2
│   └── sqlserver_connector.py     # SQL Server / Azure SQL via pyodbc
│
├── reconciliation/                 # Row comparison logic
│   └── dataset_comparator.py      # Outer-merge + hash-based status assignment
│
├── reporting/                      # Report generation
│   └── report_writer.py           # Summary CSV, combined CSV, HTML report
│
├── orchestration/                  # Run management
│   ├── run_logger.py               # Dual console + file logger
│   └── devops_publisher.py        # Azure DevOps Test Run publisher
│
├── utils/                          # Shared helpers
│   └── dataframe_helpers.py       # Header normalisation, file I/O
│
├── config/                         # Run configuration (Excel + SQL files)
│   ├── master_datasets.xlsx        # Dataset catalogue (which datasets to run)
│   ├── connection_registry.xlsx    # Database connection definitions
│   ├── mappings/                   # Schema mapping files per dataset
│   ├── idmaps/                     # Lookup / ID translation tables
│   └── queries/
│       ├── origin/                 # SQL files for the origin system
│       └── destination/            # SQL files for the destination system
│
├── local/                          # CSV fixtures for local/CI mode
├── outputs/
│   ├── reports/                    # HTML + CSV audit reports (per dataset)
│   ├── extracts/                   # Raw DB extract snapshots
│   └── logs/                       # Timestamped run logs
│
├── .env.example                    # Environment variable template
├── requirements.txt
└── README.md
```

---

## Quick Start

### 1. Clone and install

```bash
git clone https://github.com/your-username/datasync-audit.git
cd datasync-audit
pip install -r requirements.txt
```

### 2. Configure environment variables

```bash
cp .env.example .env
# Edit .env — set database credentials and (optionally) ADO details
```

### 3. Add configuration files

Place the following Excel files in `config/`:

| File | Purpose |
|---|---|
| `master_datasets.xlsx` | Lists which datasets to reconcile and their associated files |
| `connection_registry.xlsx` | Database connection parameters (host, port, user, password) |
| `mappings/<dataset>.xlsx` | Schema mapping rules per dataset |
| `idmaps/<dataset>.xlsx` | ID lookup tables (if using the `idmap` rule) |

Place your SQL query files in `config/queries/origin/` and `config/queries/destination/`.

See `config/mapping_template.md` for full column documentation.

### 4. Run

```bash
# Live database connections
python main.py

# Local CSV fixtures (for dev/CI, no DB required)
python main.py --local

# Skip Azure DevOps publishing (reports saved locally only)
python main.py --skip-publish

# Local mode + no ADO upload
python main.py --local --skip-publish
```

---

## Configuration Files

### `config/master_datasets.xlsx`

| Column | Description |
|---|---|
| `enabled` | `YES` (live DB), `LOCAL` (CSV fixtures), or `NO` (skip) |
| `dataset_name` | Human-readable label used in reports and ADO |
| `origin_db_key` | Key referencing a row in `connection_registry.xlsx` |
| `destination_db_key` | Key referencing a row in `connection_registry.xlsx` |
| `origin_query_file` | SQL filename (DB mode) or CSV filename (local mode) |
| `destination_query_file` | SQL filename or CSV filename |
| `mapping_file` | Path under `config/` to the Excel mapping file |
| `lookup_file` | Path to an optional ID lookup Excel file |

### `config/connection_registry.xlsx`

| Column | Description |
|---|---|
| `db_key` | Unique identifier referenced by `master_datasets.xlsx` |
| `db_type` | `postgresql`, `sqlserver`, or `azuresql` |
| `host` | Server hostname |
| `port` | Port number |
| `database` | Database name |
| `user` | Username |
| `password` | Password |
| `driver` | ODBC driver name (SQL Server / Azure SQL only) |

---

## Reconciliation Statuses

| Status | Meaning |
|---|---|
| `MATCH` | Row exists in both systems with identical non-key values |
| `MISMATCH` | Row exists in both but one or more values differ |
| `MISSING_IN_DEST` | Row exists in origin but is absent from destination |
| `EXTRA_IN_DEST` | Row exists in destination but has no matching origin row |

---

## Transformation Rules

| Rule | Description |
|---|---|
| `direct` | Pass value through unchanged |
| `bool_to_int` | Convert `TRUE`/`FALSE` → `1`/`0` |
| `idmap` | Translate IDs via a lookup table (configurable via JSON params) |
| `strip_prefix` | Remove environment/batch prefixes and normalise to uppercase |

See `config/mapping_template.md` for detailed parameter documentation.

---

## Azure DevOps Integration

Set the following environment variables in `.env` to enable live publishing:

```dotenv
ADO_ORG_URL=https://dev.azure.com/your-org
ADO_PROJECT=YourProject
ADO_PLAN_ID=12345
ADO_SUITE_ID=67890
ADO_PAT=your-pat
```

Results are published to an ADO Test Run immediately after each dataset's reports are generated (streaming, not batched). Each dataset is matched to an ADO Test Case by a case-insensitive substring match on the dataset name.

Works with both **Azure DevOps Services** (cloud) and **Azure DevOps Server** (on-premises).

---

## Example Output

```
outputs/
└── reports/
    └── employees/
        ├── summary_20260601_120000.csv
        ├── combined_20260601_120000.csv
        └── report_20260601_120000.html
```

The HTML report is colour-coded:
- 🟢 **MATCH** — green
- 🔴 **MISMATCH** — red
- 🟠 **MISSING_IN_DEST** — orange
- 🔵 **EXTRA_IN_DEST** — blue

---

## Requirements

- Python 3.11+
- PostgreSQL ODBC: `psycopg2-binary`
- SQL Server ODBC: `pyodbc` + [ODBC Driver 18 for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server)

---

## License

MIT License — see `LICENSE` for details.
