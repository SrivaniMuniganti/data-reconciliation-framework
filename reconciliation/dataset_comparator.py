"""
reconciliation/dataset_comparator.py
--------------------------------------
Performs row-by-row reconciliation between the transformed origin dataset
(expected state) and the transformed destination dataset (actual state).

Reconciliation Status Definitions
-----------------------------------
MATCH               Row exists in both datasets with identical non-key values.
MISMATCH            Row exists in both datasets but one or more values differ.
MISSING_IN_DEST     Row exists in origin but is absent from destination (data loss).
EXTRA_IN_DEST       Row exists in destination but has no matching origin row.

Algorithm
---------
1. Attach ``_origin_*`` columns from the pre-transformation origin data (if
   supplied) so that analysts can trace discrepancies back to the raw source.
2. Compute a SHA-256 hash of all non-key column values for both datasets.
3. Outer-merge the two datasets on the configured join key columns.
4. Assign a reconciliation status to each merged row based on which side(s)
   contributed the row and whether the hashes agree.
"""

import hashlib
import pandas as pd


class DatasetComparator:
    """
    Reconciles two normalised DataFrames and classifies every row.

    Parameters
    ----------
    schema_parser : SchemaParser
        Provides the join key columns and origin→destination column map.

    Raises
    ------
    ValueError
        If no key columns are defined in the mapping file.
    """

    def __init__(self, schema_parser):
        self.key_columns = schema_parser.get_key_columns()
        self.column_map = schema_parser.get_mappings()

        if not self.key_columns:
            raise ValueError(
                "No key columns are defined in the schema mapping. "
                "At least one column must have Is_Key=Y to enable row alignment."
            )

        print(f"     🔑 Comparator join keys: {self.key_columns}")

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def reconcile(
        self,
        df_expected: pd.DataFrame,
        df_actual: pd.DataFrame,
        df_origin_raw: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """
        Reconcile expected vs actual datasets and return a status-annotated DataFrame.

        Parameters
        ----------
        df_expected : pd.DataFrame
            Transformed origin data — the reference state we expect to find.
        df_actual : pd.DataFrame
            Transformed destination data — what actually exists after migration.
        df_origin_raw : pd.DataFrame, optional
            Pre-transformation origin data. When provided, original column values
            are included in the output for audit trail purposes (``_origin_*`` prefix).

        Returns
        -------
        pd.DataFrame
            Combined DataFrame containing:
            - Join key columns
            - ``_origin_*`` columns  (when df_origin_raw is supplied)
            - Data columns with ``_expected`` / ``_actual`` suffixes
            - ``_hash_expected`` / ``_hash_actual`` (debugging)
            - ``reconciliation_status`` : MATCH | MISMATCH | MISSING_IN_DEST | EXTRA_IN_DEST
        """
        print(
            f"     🔍 Reconciling {len(df_expected)} expected rows "
            f"against {len(df_actual)} actual rows"
        )

        # ── Attach origin audit columns ─────────────────────────────────────
        if df_origin_raw is not None:
            print("     ✨ Origin tracking enabled — attaching raw source columns")
            df_origin_raw = df_origin_raw.copy()
            df_origin_raw.columns = (
                df_origin_raw.columns.astype(str).str.strip().str.lower()
            )

            for src_col, _tgt_col in self.column_map.items():
                audit_col = f"_origin_{src_col}"
                src_lower = src_col.lower()
                if src_lower in df_origin_raw.columns:
                    df_expected[audit_col] = df_origin_raw[src_lower].astype(str)
                else:
                    df_expected[audit_col] = ""

        # ── Coerce all values to string ─────────────────────────────────────
        df_expected = df_expected.astype(str)
        df_actual = df_actual.astype(str)

        # ── Ensure key columns exist on both sides ──────────────────────────
        for key in self.key_columns:
            if key not in df_expected.columns:
                print(f"     ⚠️  Join key '{key}' absent from expected data — padding with empty string")
                df_expected[key] = ""
            if key not in df_actual.columns:
                print(f"     ⚠️  Join key '{key}' absent from actual data — padding with empty string")
                df_actual[key] = ""

        # ── Compute row hashes ──────────────────────────────────────────────
        df_expected["_hash"] = df_expected.apply(
            lambda r: self._row_hash(r, self.key_columns), axis=1
        )
        df_actual["_hash"] = df_actual.apply(
            lambda r: self._row_hash(r, self.key_columns), axis=1
        )

        # ── Outer merge ─────────────────────────────────────────────────────
        print(f"     🔗 Merging on key columns: {self.key_columns}")
        merged = pd.merge(
            df_expected,
            df_actual,
            on=self.key_columns,
            how="outer",
            suffixes=("_expected", "_actual"),
            indicator=True,
        )
        print(f"     📊 Merged row count: {len(merged)}")

        # ── Assign reconciliation status ────────────────────────────────────
        merged["reconciliation_status"] = merged.apply(self._assign_status, axis=1)

        status_counts = merged["reconciliation_status"].value_counts()
        print("     📈 Reconciliation results:")
        for status, count in status_counts.items():
            print(f"        {status}: {count}")

        # ── Reorder: keys → origin audit → data columns → hashes → status ──
        audit_cols = [c for c in merged.columns if c.startswith("_origin_")]
        if audit_cols:
            other_cols = [c for c in merged.columns if not c.startswith("_origin_")]
            present_keys = [k for k in self.key_columns if k in merged.columns]
            remaining = [c for c in other_cols if c not in present_keys and c != "_merge"]
            merged = merged[[c for c in present_keys + audit_cols + remaining if c in merged.columns]]
            print(f"     ✨ Attached {len(audit_cols)} origin audit columns")

        if "_merge" in merged.columns:
            merged = merged.drop(columns=["_merge"])

        return merged

    # -------------------------------------------------------------------------
    # Helpers
    # -------------------------------------------------------------------------

    def _row_hash(self, row: pd.Series, key_cols: list) -> str:
        """
        Generate a SHA-256 hash of all non-key, non-audit column values.

        The hash enables O(1) detection of value differences without comparing
        each column individually.
        """
        non_key_values = [
            str(row[c])
            for c in row.index
            if c not in key_cols and not c.startswith("_origin_")
        ]
        combined = "||".join(non_key_values)
        return hashlib.sha256(combined.encode()).hexdigest()

    def _assign_status(self, row: pd.Series) -> str:
        """Determine reconciliation status from the merge indicator and hash comparison."""
        merge_flag = row["_merge"]

        if merge_flag == "left_only":
            return "MISSING_IN_DEST"

        if merge_flag == "right_only":
            return "EXTRA_IN_DEST"

        if row.get("_hash_expected") == row.get("_hash_actual"):
            return "MATCH"

        return "MISMATCH"
