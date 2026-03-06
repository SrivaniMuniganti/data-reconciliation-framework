"""
reporting/report_writer.py
---------------------------
Generates validation reports from reconciliation results.

Three artefacts are produced per dataset:

1. **Summary CSV**   – Aggregated counts per reconciliation status.
2. **Combined CSV**  – Full row-by-row comparison data for deep-dive analysis.
3. **HTML Report**   – Colour-coded, browser-viewable report for stakeholders.

The HTML report includes:
- A legend explaining each status colour.
- Summary statistics table.
- Full interactive row-level comparison table.
"""

import os
import pandas as pd


# Status → background colour mapping (Tailwind-inspired palette)
_STATUS_COLOURS = {
    "MATCH": "#C6F6D5",           # green
    "MISMATCH": "#FED7D7",        # red
    "MISSING_IN_DEST": "#FEEBC8", # orange
    "EXTRA_IN_DEST": "#BEE3F8",   # blue
}

# Canonical status ordering for consistent summary display
_STATUS_ORDER = ["MATCH", "MISMATCH", "MISSING_IN_DEST", "EXTRA_IN_DEST"]


class ReportWriter:
    """
    Produces CSV and HTML audit reports from a reconciliation result DataFrame.
    """

    @staticmethod
    def write(
        df: pd.DataFrame,
        output_dir: str,
        run_timestamp: str,
        dataset_label: str | None = None,
    ) -> None:
        """
        Write summary CSV, combined CSV, and HTML report to ``output_dir``.

        Parameters
        ----------
        df : pd.DataFrame
            Reconciliation output from ``DatasetComparator.reconcile()``.
            Must contain a ``reconciliation_status`` column.
        output_dir : str
            Directory where the three report files will be written.
            Created automatically if it does not exist.
        run_timestamp : str
            Timestamp string appended to filenames (e.g. ``20260601_120000``).
        dataset_label : str, optional
            Human-readable dataset/entity name used in the report header.
            Falls back to the ``output_dir`` basename when not provided.
        """
        os.makedirs(output_dir, exist_ok=True)
        print(f"     📁 Output directory: {output_dir}")

        if dataset_label is None:
            dataset_label = os.path.basename(output_dir)

        # ── Clean and reorder columns ─────────────────────────────────────
        df_clean = ReportWriter._clean_columns(df)

        # ── 1. Summary CSV ─────────────────────────────────────────────────
        summary = ReportWriter._build_summary(df_clean)
        summary_path = os.path.join(output_dir, f"summary_{run_timestamp}.csv")
        summary.to_csv(summary_path, index=False)
        print(f"     📄 Summary CSV  → {summary_path}")

        # ── 2. Combined CSV ────────────────────────────────────────────────
        combined_path = os.path.join(output_dir, f"combined_{run_timestamp}.csv")
        df_clean.to_csv(combined_path, index=False)
        print(f"     📄 Combined CSV → {combined_path}")

        # ── 3. HTML Report ─────────────────────────────────────────────────
        html_path = os.path.join(output_dir, f"report_{run_timestamp}.html")
        ReportWriter._write_html(df_clean, summary, html_path, run_timestamp, dataset_label)
        print(f"     📊 HTML Report  → {html_path}")

        # ── Console summary ────────────────────────────────────────────────
        print(f"\n     {'='*60}")
        print(f"     🎯 REPORTS COMPLETE — {dataset_label}")
        print(f"     {'='*60}")
        for _, row in summary.iterrows():
            print(f"        {row['reconciliation_status']:25s}: {row['count']:6d} rows")
        print(f"     {'='*60}\n")

    # -------------------------------------------------------------------------
    # Internal helpers
    # -------------------------------------------------------------------------

    @staticmethod
    def _clean_columns(df: pd.DataFrame) -> pd.DataFrame:
        """
        Remove redundant ``_expected`` columns and rename ``_actual`` columns
        for a cleaner report layout.

        Column order: origin audit → key columns → data columns → hashes → status.
        """
        df_clean = df.copy()

        # Drop _expected value columns (keep only _hash_expected)
        drop_cols = [
            c for c in df_clean.columns
            if c.endswith("_expected") and c != "_hash_expected"
        ]
        if drop_cols:
            df_clean = df_clean.drop(columns=drop_cols)

        # Rename _actual value columns (keep only _hash_actual as-is)
        rename_map = {
            c: c.replace("_actual", "")
            for c in df_clean.columns
            if c.endswith("_actual") and c != "_hash_actual"
        }
        if rename_map:
            df_clean = df_clean.rename(columns=rename_map)

        # Reorder columns
        origin_cols = [c for c in df_clean.columns if c.startswith("_origin_")]
        hash_cols = [c for c in df_clean.columns if c in ("_hash_expected", "_hash_actual")]
        status_col = ["reconciliation_status"] if "reconciliation_status" in df_clean.columns else []
        remaining = [
            c for c in df_clean.columns
            if c not in origin_cols and c not in hash_cols and c not in status_col
        ]

        final_order = [c for c in origin_cols + remaining + hash_cols + status_col if c in df_clean.columns]
        missing = [c for c in df_clean.columns if c not in final_order]
        df_clean = df_clean[final_order + missing]

        return df_clean

    @staticmethod
    def _build_summary(df: pd.DataFrame) -> pd.DataFrame:
        """Return a sorted summary DataFrame with status counts."""
        summary = df["reconciliation_status"].value_counts().reset_index()
        summary.columns = ["reconciliation_status", "count"]
        summary["reconciliation_status"] = pd.Categorical(
            summary["reconciliation_status"],
            categories=_STATUS_ORDER,
            ordered=True,
        )
        return summary.sort_values("reconciliation_status").reset_index(drop=True)

    @staticmethod
    def _write_html(
        df: pd.DataFrame,
        summary: pd.DataFrame,
        html_path: str,
        timestamp: str,
        label: str,
    ) -> None:
        """Render and save the colour-coded HTML report."""

        def _row_colour(row: pd.Series):
            colour = _STATUS_COLOURS.get(row["reconciliation_status"], "white")
            return [f"background-color: {colour}"] * len(row)

        styled = (
            df.style
            .apply(_row_colour, axis=1)
            .set_table_styles([
                {"selector": "th", "props": "background-color:#1A365D; color:white; padding:8px; font-weight:bold; text-align:left;"},
                {"selector": "td", "props": "padding:6px; font-size:13px; border:1px solid #ddd;"},
                {"selector": "table", "props": "border-collapse:collapse; width:100%; margin-top:20px;"},
            ])
            .hide(axis="index")
        )

        html_table = styled.to_html()

        html_doc = f"""<!DOCTYPE html>
<html lang="en">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>DataSync Audit — {label}</title>
    <style>
        body {{
            font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;
            padding: 30px;
            background: #f7f9fc;
            color: #2D3748;
        }}
        h1 {{ color: #1A365D; border-bottom: 3px solid #1A365D; padding-bottom: 10px; }}
        h3 {{ color: #2D3748; margin-top: 25px; }}
        .dataset-label {{ color: #2B6CB0; font-size: 1.3em; font-weight: 600; margin: 10px 0; }}
        .legend-container {{
            margin: 20px 0; padding: 15px;
            background: white; border-radius: 8px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .legend-box {{
            display: inline-block; margin: 5px 10px 5px 0;
            padding: 10px 15px; border-radius: 6px;
            font-size: 14px; font-weight: 500; border: 2px solid #ddd;
        }}
        .card {{
            margin: 20px 0; background: white;
            border-radius: 8px; padding: 15px;
            box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        }}
        .card.overflow {{ overflow-x: auto; }}
        .timestamp {{ color: #718096; font-size: 14px; }}
        footer {{ text-align: center; color: #718096; font-size: 12px; margin-top: 40px; }}
    </style>
</head>
<body>
    <h1>🔍 DataSync Audit — Cross-System Data Reconciliation Report</h1>
    <div class="dataset-label">Dataset: {label}</div>
    <p class="timestamp">Generated: {timestamp}</p>

    <div class="legend-container">
        <h3>Reconciliation Status Legend</h3>
        <span class="legend-box" style="background:#C6F6D5;">✓ MATCH — Values are identical</span>
        <span class="legend-box" style="background:#FED7D7;">✗ MISMATCH — Values differ</span>
        <span class="legend-box" style="background:#FEEBC8;">⚠ MISSING_IN_DEST — Row absent from destination</span>
        <span class="legend-box" style="background:#BEE3F8;">+ EXTRA_IN_DEST — Unexpected row in destination</span>
    </div>

    <div class="card">
        <h3>Summary Statistics</h3>
        {summary.to_html(index=False, border=0)}
    </div>

    <div class="card overflow">
        <h3>Detailed Row-by-Row Comparison</h3>
        {html_table}
    </div>

    <br>
    <footer>
        DataSync Audit Framework &nbsp;|&nbsp; Dataset: {label} &nbsp;|&nbsp; Run: {timestamp}
    </footer>
</body>
</html>
"""
        with open(html_path, "w", encoding="utf-8") as fh:
            fh.write(html_doc)
