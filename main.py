"""
main.py
-------
DataSync Audit — Cross-System Data Reconciliation Framework
CLI entry point and main pipeline orchestrator.

This module drives the full reconciliation pipeline:
    1. Load run configuration (dataset catalogue + connection registry).
    2. Extract data from origin and destination systems (or local CSV files).
    3. Parse schema mappings and apply transformation rules.
    4. Reconcile origin vs destination data row-by-row.
    5. Generate summary CSV, combined CSV, and HTML audit reports.
    6. Publish results to Azure DevOps Test Run (optional).
    7. Print a consolidated run summary.

Usage
-----
    python main.py                         # Live DB connections
    python main.py --local                 # Use local CSV fixtures
    python main.py --skip-publish          # Skip Azure DevOps upload
    python main.py --local --skip-publish  # Local CSV + no ADO upload
"""

import argparse
import datetime
import glob
import os
import traceback

import pandas as pd

from connectors import PostgresConnector, SqlServerConnector
from core import SchemaParser, TransformEngine
from orchestration import RunLogger, DevOpsPublisher
from reconciliation import DatasetComparator
from reporting import ReportWriter

# ---------------------------------------------------------------------------
# Directory constants
# ---------------------------------------------------------------------------
CONFIG_DIR = "config"
LOCAL_DIR = "local"
OUTPUT_ROOT = os.path.join("outputs", "reports")
LOG_DIR = os.path.join("outputs", "logs")
EXTRACT_DIR = os.path.join("outputs", "extracts")


# ---------------------------------------------------------------------------
# Configuration loaders
# ---------------------------------------------------------------------------

def _load_dataset_catalogue(logger: RunLogger) -> pd.DataFrame:
    """
    Load the dataset catalogue (master_tables.xlsx equivalent).

    Expected columns
    ----------------
    enabled             YES / NO / LOCAL — controls which datasets run.
    dataset_name        Human-readable dataset identifier.
    origin_db_key       References a row in the connection registry.
    destination_db_key  References a row in the connection registry.
    origin_query_file   SQL file (DB mode) or CSV filename (local mode).
    destination_query_file
    mapping_file        Excel schema-mapping file under config/.
    lookup_file         Excel ID-lookup file under config/ (optional).
    """
    path = os.path.join(CONFIG_DIR, "master_datasets.xlsx")
    logger.info(f"Loading dataset catalogue: {path}")
    return pd.read_excel(path)


def _load_connection_registry(logger: RunLogger) -> pd.DataFrame:
    """
    Load the database connection registry (db_servers.xlsx equivalent).

    Expected columns
    ----------------
    db_key      Unique connection identifier.
    db_type     postgresql | sqlserver | azuresql.
    host        Server hostname.
    port        Port number.
    database    Database name.
    user        Username.
    password    Password.
    driver      ODBC driver (SQL Server / Azure SQL only).
    """
    path = os.path.join(CONFIG_DIR, "connection_registry.xlsx")
    logger.info(f"Loading connection registry: {path}")
    return pd.read_excel(path)


# ---------------------------------------------------------------------------
# Data loading
# ---------------------------------------------------------------------------

def _load_local_csv(filename: str, label: str, logger: RunLogger) -> pd.DataFrame:
    """Load a named CSV file from the local fixtures directory."""
    path = os.path.join(LOCAL_DIR, filename)
    logger.info(f"Loading local {label}: {path}")
    if not os.path.exists(path):
        raise FileNotFoundError(f"Local fixture not found: {path}")
    return pd.read_csv(path)


def _load_from_database(db_config: dict, query_path: str, logger: RunLogger) -> pd.DataFrame:
    """Execute a SQL file against the specified database connection."""
    if not os.path.exists(query_path):
        raise FileNotFoundError(f"Query file not found: {query_path}")

    with open(query_path, encoding="utf-8") as fh:
        sql = fh.read()

    db_type = str(db_config.get("db_type", "")).lower()
    logger.info(f"Executing query: {os.path.basename(query_path)}")

    if db_type in ("postgres", "postgresql"):
        return PostgresConnector.fetch(db_config, sql)

    if db_type in ("azuresql", "azure_sql", "sqlserver", "mssql"):
        return SqlServerConnector.fetch(db_config, sql)

    raise ValueError(f"Unsupported db_type: '{db_type}'")


# ---------------------------------------------------------------------------
# ADO helpers
# ---------------------------------------------------------------------------

def _ado_env_configured(logger: RunLogger) -> bool:
    """Return True if all required ADO environment variables are present."""
    required = ["ADO_ORG_URL", "ADO_PROJECT", "ADO_PLAN_ID", "ADO_SUITE_ID", "ADO_PAT"]
    missing = [v for v in required if not os.environ.get(v)]
    if missing:
        logger.warning(
            f"Azure DevOps upload disabled — missing env vars: {missing}\n"
            "  Set ADO_ORG_URL, ADO_PROJECT, ADO_PLAN_ID, ADO_SUITE_ID, ADO_PAT to enable."
        )
        return False
    return True


def _open_devops_run(run_name: str, logger: RunLogger):
    """Create a DevOpsPublisher and open a Test Run. Returns (publisher, run_id) or (None, None)."""
    if not _ado_env_configured(logger):
        return None, None
    try:
        publisher = DevOpsPublisher(logger=logger)
        run_id = publisher.open_run(run_name)
        return publisher, run_id
    except Exception as exc:
        logger.warning(f"ADO run creation failed (reports saved locally): {exc}")
        return None, None


def _publish_dataset(publisher, run_id, dataset_name, status_summary, duration, out_dir, timestamp, logger):
    """Publish a single dataset result to ADO immediately after reports are written."""
    if publisher is None or run_id is None:
        return None

    csv_candidates = sorted(glob.glob(os.path.join(out_dir, f"*{timestamp}*.csv")), key=os.path.getmtime, reverse=True)
    html_candidates = sorted(glob.glob(os.path.join(out_dir, f"*{timestamp}*.html")), key=os.path.getmtime, reverse=True)

    csv_path = csv_candidates[0] if csv_candidates else None
    html_path = html_candidates[0] if html_candidates else None

    try:
        return publisher.publish_dataset_result(
            run_id=run_id,
            dataset_name=dataset_name,
            status_summary=status_summary,
            duration_seconds=duration,
            csv_path=csv_path,
            html_path=html_path,
        )
    except Exception as exc:
        logger.warning(f"ADO publish failed for '{dataset_name}' (reports still saved): {exc}")
        return None


def _close_devops_run(publisher, run_id, logger):
    """Close the ADO run and return its summary dict."""
    if publisher is None or run_id is None:
        return None
    try:
        return publisher.close_run(run_id)
    except Exception as exc:
        logger.warning(f"ADO run close failed: {exc}")
        return None


def _build_run_name(timestamp: str) -> str:
    """Build the ADO Test Run display name from environment variables."""
    batch = os.environ.get("BATCH_NAME", "BATCH").strip().strip('"').strip("'")
    env = os.environ.get("ENV", "_").strip().strip('"').strip("'")
    return f"{batch} | {env} | {timestamp}"


# ---------------------------------------------------------------------------
# Summary reporting
# ---------------------------------------------------------------------------

def _print_run_summary(
    logger: RunLogger,
    all_datasets: list,
    succeeded: list,
    failed: list,
    entity_rows: list,
    ado_run_result,
    start_time: datetime.datetime,
    end_time: datetime.datetime,
    local_mode: bool,
) -> None:
    """Print a consolidated summary table covering all processed datasets."""
    duration = end_time - start_time
    logger.banner("CONSOLIDATED RUN SUMMARY")

    logger.summary("Execution", {
        "Mode": "LOCAL CSV" if local_mode else "LIVE DB",
        "Start": start_time.strftime("%Y-%m-%d %H:%M:%S"),
        "End": end_time.strftime("%Y-%m-%d %H:%M:%S"),
        "Duration": str(duration).split(".")[0],
        "Total datasets": len(all_datasets),
        "Succeeded": len(succeeded),
        "Failed": len(failed),
    })

    if entity_rows:
        col_name = max(len(r["dataset"]) for r in entity_rows)
        col_name = max(col_name, 7)

        header = (
            f"{'Dataset':<{col_name}}  {'Outcome':<8}  {'Match %':>8}  "
            f"{'Mismatch':>9}  {'Missing':>9}  {'Extra':>7}  "
            f"{'Total':>7}  {'Duration':>10}  {'ADO':^12}"
        )
        divider = "─" * len(header)

        logger.info("")
        logger.info("  Per-Dataset Results")
        logger.info(f"  {divider}")
        logger.info(f"  {header}")
        logger.info(f"  {divider}")

        for r in entity_rows:
            ado_mark = "✅ published" if r["ado_published"] else "⚠️  skipped"
            logger.info(
                f"  {r['dataset']:<{col_name}}  "
                f"{r['outcome']:<8}  {r['match_pct']:>8}%  "
                f"{r['mismatches']:>9}  {r['missing']:>9}  {r['extra']:>7}  "
                f"{r['total']:>7}  {r['duration_seconds']:>9.1f}s  "
                f"{ado_mark:^12}"
            )

        logger.info(f"  {divider}")
        logger.info("")

    if failed:
        logger.warning("Failed datasets:")
        for name, err in failed:
            logger.error(f"  ✗ {name}: {err}")
        logger.info("")

    if ado_run_result:
        logger.success(f"Azure DevOps Test Run → {ado_run_result['run_url']}")
    else:
        logger.info("Azure DevOps Test Run → not published (ADO not configured or disabled)")

    logger.info(f"Reports directory: {OUTPUT_ROOT}")
    logger.info("")


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

def main() -> int:
    """Entry point for the DataSync Audit reconciliation pipeline."""
    logger = RunLogger(LOG_DIR, "datasync_audit")

    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    try:
        parser = argparse.ArgumentParser(
            prog="datasync-audit",
            description=(
                "DataSync Audit — Cross-System Data Reconciliation Framework.\n"
                "Compares data between an origin and a destination system, "
                "applies schema mappings and transformation rules, and produces "
                "detailed audit reports."
            ),
        )
        parser.add_argument(
            "--local",
            action="store_true",
            help="Run using local CSV fixtures instead of live database connections.",
        )
        parser.add_argument(
            "--skip-publish",
            action="store_true",
            help="Skip Azure DevOps Test Run publishing; save reports locally only.",
        )
        args = parser.parse_args()

        local_mode = args.local
        skip_publish = args.skip_publish

        mode_label = (
            ("LOCAL CSV" if local_mode else "LIVE DB")
            + (" | ADO SKIPPED" if skip_publish else "")
        )
        logger.banner(f"DATASYNC AUDIT — RECONCILIATION RUN STARTED → MODE: {mode_label}")
        logger.info(f"Log file: {logger.log_file}")

        start_time = datetime.datetime.now()

        # ── Step 1: Load configuration ─────────────────────────────────────
        logger.step(1, 7, "Loading run configuration")
        catalogue = _load_dataset_catalogue(logger)

        if local_mode:
            connection_registry = None
            logger.info("Skipping connection registry (local mode)")
        else:
            logger.step(2, 7, "Loading connection registry")
            connection_registry = _load_connection_registry(logger)

        run_timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        logger.info(f"Run timestamp: {run_timestamp}")

        # ── Step 3: Resolve enabled datasets ──────────────────────────────
        logger.step(3, 7, "Resolving enabled datasets")
        if local_mode:
            enabled_flag = ("LOCAL", "CSV")
        else:
            enabled_flag = ("YES", "TRUE", "1")

        datasets = [
            row for _, row in catalogue.iterrows()
            if str(row["enabled"]).upper() in enabled_flag
        ]

        logger.info(f"Datasets to process: {len(datasets)}")
        for i, ds in enumerate(datasets, 1):
            logger.info(f"  {i}. {ds['dataset_name']}")

        # ── Step 4: Open ADO run ────────────────────────────────────────────
        logger.step(4, 7, "Opening Azure DevOps Test Run")
        if skip_publish:
            logger.info("--skip-publish set: Azure DevOps publishing disabled")
            publisher, ado_run_id = None, None
        else:
            run_name = _build_run_name(run_timestamp)
            logger.info(f"ADO run name: {run_name}")
            publisher, ado_run_id = _open_devops_run(run_name, logger)

        # ── Step 5: Process datasets ────────────────────────────────────────
        logger.step(5, 7, "Processing datasets")
        succeeded = []
        failed = []
        entity_rows = []

        for idx, row in enumerate(datasets, 1):
            dataset_name = row["dataset_name"]
            logger.dataset_start(f"{dataset_name} ({idx}/{len(datasets)})")
            ds_start = datetime.datetime.now()

            try:
                # 5a. Load raw data
                logger.step(1, 7, "Loading origin and destination data")

                if local_mode:
                    df_origin = pd.read_csv(os.path.join(LOCAL_DIR, row["origin_query_file"]))
                    df_destination = pd.read_csv(os.path.join(LOCAL_DIR, row["destination_query_file"]))
                else:
                    origin_cfg = connection_registry[
                        connection_registry["db_key"] == row["origin_db_key"]
                    ].iloc[0].to_dict()
                    dest_cfg = connection_registry[
                        connection_registry["db_key"] == row["destination_db_key"]
                    ].iloc[0].to_dict()

                    df_origin = _load_from_database(
                        origin_cfg,
                        os.path.join(CONFIG_DIR, "queries", "origin", row["origin_query_file"]),
                        logger,
                    )
                    df_destination = _load_from_database(
                        dest_cfg,
                        os.path.join(CONFIG_DIR, "queries", "destination", row["destination_query_file"]),
                        logger,
                    )

                logger.info(f"Origin rows loaded      : {len(df_origin)}")
                logger.info(f"Destination rows loaded : {len(df_destination)}")

                # 5b. Preserve raw origin for audit trail
                logger.step(2, 7, "Preserving raw origin data for audit trail")
                df_origin_raw = df_origin.copy()

                # 5c. Parse schema mapping
                logger.step(3, 7, "Parsing schema mapping")
                mapping_path = os.path.join(CONFIG_DIR, row["mapping_file"])
                logger.info(f"Schema mapping: {mapping_path}")
                schema = SchemaParser(mapping_path)

                # 5d. Load lookup / ID table (optional)
                logger.step(4, 7, "Loading lookup table (if configured)")
                _lookup_val = row.get("lookup_file", "")
                _lookup_val = "" if pd.isna(_lookup_val) else str(_lookup_val).strip()
                lookup_path = os.path.join(CONFIG_DIR, _lookup_val) if _lookup_val else ""
                if lookup_path and os.path.exists(lookup_path):
                    logger.info(f"Lookup file: {lookup_path}")
                    lookup_df = pd.read_excel(lookup_path)
                else:
                    logger.info("No lookup file configured for this dataset")
                    lookup_df = None

                # 5e. Apply transformations
                logger.step(5, 7, "Applying transformation rules")
                engine = TransformEngine(schema, lookup_df)
                df_expected = engine.transform(df_origin, "origin")
                df_actual = engine.transform(df_destination, "destination")

                # 5f. Reconcile
                logger.step(6, 7, "Reconciling origin vs destination")
                comparator = DatasetComparator(schema)
                reconciled = comparator.reconcile(df_expected, df_actual, df_origin_raw)

                status_counts = reconciled["reconciliation_status"].value_counts().to_dict()
                logger.summary(f"Reconciliation — {dataset_name}", {
                    "MATCH":           status_counts.get("MATCH", 0),
                    "MISMATCH":        status_counts.get("MISMATCH", 0),
                    "MISSING_IN_DEST": status_counts.get("MISSING_IN_DEST", 0),
                    "EXTRA_IN_DEST":   status_counts.get("EXTRA_IN_DEST", 0),
                    "Total Rows":      len(reconciled),
                })

                # 5g. Write reports
                logger.step(7, 7, "Generating audit reports")
                out_dir = os.path.join(OUTPUT_ROOT, dataset_name)
                ReportWriter.write(reconciled, out_dir, run_timestamp, dataset_label=dataset_name)
                logger.success(f"Reports written to: {out_dir}")

                ds_duration = (datetime.datetime.now() - ds_start).total_seconds()

                # 5h. Publish to ADO immediately
                ado_result = _publish_dataset(
                    publisher, ado_run_id, dataset_name,
                    status_counts, ds_duration, out_dir, run_timestamp, logger,
                )

                # Accumulate summary row
                matched = status_counts.get("MATCH", 0)
                mismatches = status_counts.get("MISMATCH", 0)
                missing = status_counts.get("MISSING_IN_DEST", 0)
                extra = status_counts.get("EXTRA_IN_DEST", 0)
                total = matched + mismatches + missing + extra
                pct = round((matched / total * 100), 1) if total else 0.0

                entity_rows.append({
                    "dataset": dataset_name,
                    "outcome": "Passed" if (mismatches == 0 and missing == 0) else "Failed",
                    "match_pct": pct,
                    "mismatches": mismatches,
                    "missing": missing,
                    "extra": extra,
                    "total": total,
                    "duration_seconds": ds_duration,
                    "ado_published": ado_result is not None,
                })

                logger.dataset_end(dataset_name, success=True)
                succeeded.append(dataset_name)

            except Exception as exc:
                logger.error(f"Error processing '{dataset_name}': {exc}")
                for line in traceback.format_exc().splitlines():
                    if line.strip():
                        logger.error(f"  {line}")
                logger.dataset_end(dataset_name, success=False)
                failed.append((dataset_name, str(exc)))

        # ── Step 6: Close ADO run ───────────────────────────────────────────
        logger.step(6, 7, "Closing Azure DevOps Test Run")
        ado_run_result = _close_devops_run(publisher, ado_run_id, logger)

        # ── Step 7: Consolidated summary ────────────────────────────────────
        logger.step(7, 7, "Generating consolidated run summary")
        end_time = datetime.datetime.now()

        _print_run_summary(
            logger=logger,
            all_datasets=datasets,
            succeeded=succeeded,
            failed=failed,
            entity_rows=entity_rows,
            ado_run_result=ado_run_result,
            start_time=start_time,
            end_time=end_time,
            local_mode=local_mode,
        )

        logger.close()
        return 0 if not failed else 1

    except Exception as exc:
        logger.error(f"Critical pipeline error: {exc}")
        for line in traceback.format_exc().splitlines():
            if line.strip():
                logger.error(f"  {line}")
        logger.close()
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
