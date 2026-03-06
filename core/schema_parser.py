"""
core/schema_parser.py
----------------------
Reads and parses Excel schema-mapping files that define how origin columns
map to destination columns and which transformation rules to apply.

Mapping File Expected Columns
------------------------------
Required:
    Source_Column   – Column name in the origin (source) dataset.
    Target_Column   – Column name in the destination (target) dataset.
    Rule_Type       – Transformation rule identifier (e.g. direct, bool_to_int).
    Is_Key          – Whether this column is part of the join key (Y/N).

Optional:
    Apply_On        – Which side the rule applies to: SOURCE | TARGET | BOTH (default).
    Parameters      – JSON object of rule-specific configuration.
    Cardinality     – Row cardinality hint: 1:1 | 1:N | N:1 (informational).
"""

import json
import pandas as pd


class SchemaParser:
    """
    Parses a column-mapping Excel file and exposes structured accessors for:

    - Column name mappings  (origin column → destination column)
    - Rule definitions      (transformation rule per origin column)
    - Join key columns      (destination column names used for row alignment)
    """

    REQUIRED_COLUMNS = ["Source_Column", "Target_Column", "Rule_Type", "Is_Key"]
    OPTIONAL_COLUMNS = ["Apply_On", "Parameters", "Cardinality"]

    def __init__(self, filepath: str):
        """
        Load and validate the schema-mapping file.

        Parameters
        ----------
        filepath : str
            Path to the Excel mapping file (.xlsx).

        Raises
        ------
        FileNotFoundError
            If the file does not exist at the given path.
        ValueError
            If any required columns are missing from the spreadsheet.
        json.JSONDecodeError
            If a ``Parameters`` cell contains malformed JSON.
        """
        print(f"     📖 Loading schema mapping: {filepath}")

        self._df = pd.read_excel(filepath).fillna("")
        print(f"     📋 Loaded {len(self._df)} mapping rules")

        self._validate_structure()

        self._column_map = self._parse_column_map()
        self._rule_map = self._parse_rule_map()
        self._key_columns = self._parse_key_columns()

    # -------------------------------------------------------------------------
    # Validation
    # -------------------------------------------------------------------------

    def _validate_structure(self) -> None:
        """Raise ValueError if any required column headers are absent."""
        missing = [c for c in self.REQUIRED_COLUMNS if c not in self._df.columns]
        if missing:
            raise ValueError(
                f"Schema mapping file is missing required columns: {missing}"
            )
        print("     ✅ Schema mapping structure is valid")

    # -------------------------------------------------------------------------
    # Parsers
    # -------------------------------------------------------------------------

    def _parse_column_map(self) -> dict:
        """Build {origin_column: destination_column} lookup."""
        col_map = {}
        for _, row in self._df.iterrows():
            src = str(row["Source_Column"]).strip()
            tgt = str(row["Target_Column"]).strip()
            if src and tgt:
                col_map[src] = tgt

        print(f"     🗺️  Resolved {len(col_map)} column mappings")
        return col_map

    def _parse_rule_map(self) -> dict:
        """Build per-column rule metadata dictionary."""
        rules = {}
        for _, row in self._df.iterrows():
            src = str(row["Source_Column"]).strip()
            if not src:
                continue

            is_key = str(row["Is_Key"]).strip().upper() in ("Y", "YES", "TRUE", "1")

            apply_on = str(row.get("Apply_On", "BOTH")).strip().upper()
            if apply_on not in ("SOURCE", "TARGET", "BOTH"):
                apply_on = "BOTH"

            cardinality = str(row.get("Cardinality", "1:1")).strip()

            params_raw = str(row.get("Parameters", "")).strip()
            try:
                params = json.loads(params_raw) if params_raw else {}
            except json.JSONDecodeError as exc:
                raise json.JSONDecodeError(
                    f"Invalid JSON in Parameters for column '{src}': {params_raw}",
                    exc.doc,
                    exc.pos,
                ) from exc

            rules[src] = {
                "rule": str(row["Rule_Type"]).strip().lower(),
                "is_key": is_key,
                "apply_on": apply_on,
                "params": params,
                "cardinality": cardinality,
            }

        key_count = sum(1 for r in rules.values() if r["is_key"])
        print(f"     🔧 Parsed {len(rules)} rule definitions ({key_count} key columns)")
        return rules

    def _parse_key_columns(self) -> list:
        """Return list of destination column names that form the join key."""
        keys = []
        for _, row in self._df.iterrows():
            is_key = str(row["Is_Key"]).strip().upper() in ("Y", "YES", "TRUE", "1")
            if is_key:
                tgt = str(row["Target_Column"]).strip()
                if tgt:
                    keys.append(tgt)

        print(f"     🔑 Join key columns: {keys}")
        return keys

    # -------------------------------------------------------------------------
    # Public accessors
    # -------------------------------------------------------------------------

    def get_column_map(self) -> dict:
        """Return the origin→destination column name mapping."""
        return self._column_map

    def get_rule_map(self) -> dict:
        """Return per-column rule definitions."""
        return self._rule_map

    def get_key_columns(self) -> list:
        """Return destination column names used as join keys."""
        return self._key_columns

    # Back-compat aliases (used by TransformEngine which calls mapping_interpreter API)
    def get_mappings(self) -> dict:
        return self._column_map

    def get_rules(self) -> dict:
        return self._rule_map
