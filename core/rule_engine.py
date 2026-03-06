"""
core/rule_engine.py
--------------------
Defines and applies column-level transformation rules.

Available Rules
---------------
direct          No transformation; pass the value through unchanged.
bool_to_int     Convert boolean-like strings (TRUE/FALSE) to integers (1/0).
                Idempotent — already-integer inputs (1/0) are preserved.
idmap           Generic ID translation via a lookup table loaded from Excel.
                Requires ``lookup_column`` and ``return_column`` in params.
                Optional ``dedupe`` strategy: first | last | most_common.
strip_prefix    Normalise prefixed system names by removing everything before
                the first underscore (or first space) and uppercasing the result.
                Useful when destination values carry environment/batch prefixes.
"""

import pandas as pd


class RuleEngine:
    """
    Applies named transformation rules to pandas Series objects.

    Parameters
    ----------
    lookup_df : pd.DataFrame, optional
        ID-mapping table used by the ``idmap`` rule. Column names are
        normalised to stripped strings on load.
    """

    def __init__(self, lookup_df: pd.DataFrame | None = None):
        self.lookup_df = lookup_df

        if self.lookup_df is not None:
            # Normalise all columns to stripped strings for safe joins
            for col in self.lookup_df.columns:
                self.lookup_df[col] = self.lookup_df[col].astype(str).str.strip()

            print(f"     🔗 Lookup table loaded — {len(self.lookup_df)} entries")

    # -------------------------------------------------------------------------
    # Public dispatch
    # -------------------------------------------------------------------------

    def apply(self, rule: str, series: pd.Series, params: dict | None = None) -> pd.Series:
        """
        Apply a named rule to a pandas Series.

        Parameters
        ----------
        rule : str
            Rule identifier (case-insensitive, whitespace-stripped).
        series : pd.Series
            Input data column.
        params : dict, optional
            Rule-specific configuration parameters.

        Returns
        -------
        pd.Series
            Transformed data column.

        Raises
        ------
        ValueError
            If the rule name is not recognised.
        """
        rule = (rule or "").lower().strip()
        params = params or {}

        if rule in ("", "direct"):
            return series

        if rule == "bool_to_int":
            return self._bool_to_int(series)

        if rule == "idmap":
            return self._idmap(series, params)

        if rule == "strip_prefix":
            return self._strip_prefix(series)

        raise ValueError(f"Unknown transformation rule: '{rule}'")

    # -------------------------------------------------------------------------
    # Rule implementations
    # -------------------------------------------------------------------------

    def _bool_to_int(self, series: pd.Series) -> pd.Series:
        """
        Convert boolean-like values to integers.

        Mapping
        -------
        TRUE  / true  / 1  → 1
        FALSE / false / 0  → 0
        Any other value    → 0
        """
        normalised = series.astype(str).str.strip().str.upper()
        return normalised.map({"TRUE": 1, "FALSE": 0, "1": 1, "0": 0}).fillna(0)

    def _idmap(self, series: pd.Series, params: dict) -> pd.Series:
        """
        Translate values using a pre-loaded lookup table.

        Required params
        ---------------
        lookup_column : str   Column in lookup_df to match against.
        return_column : str   Column in lookup_df whose value is returned.

        Optional params
        ---------------
        dedupe : str          De-duplication strategy when lookup_column is
                              not unique: ``first`` | ``last`` | ``most_common``.
        """
        if self.lookup_df is None:
            raise RuntimeError(
                "The 'idmap' rule requires a lookup table, but none was provided."
            )

        lookup_col = params.get("lookup_column")
        return_col = params.get("return_column")
        dedupe = (params.get("dedupe") or "").lower()

        if not lookup_col or not return_col:
            raise ValueError(
                f"'idmap' rule requires 'lookup_column' and 'return_column' in params. "
                f"Received: {params}"
            )

        if lookup_col not in self.lookup_df.columns:
            raise KeyError(
                f"Lookup table has no column '{lookup_col}'. "
                f"Available: {list(self.lookup_df.columns)}"
            )

        if return_col not in self.lookup_df.columns:
            raise KeyError(
                f"Lookup table has no column '{return_col}'. "
                f"Available: {list(self.lookup_df.columns)}"
            )

        subset = self.lookup_df[[lookup_col, return_col]].copy()

        if dedupe == "first":
            subset = subset.drop_duplicates(subset=[lookup_col], keep="first")
        elif dedupe == "last":
            subset = subset.drop_duplicates(subset=[lookup_col], keep="last")
        elif dedupe == "most_common":
            subset = (
                subset.groupby(lookup_col)[return_col]
                .agg(lambda x: x.value_counts().idxmax())
                .reset_index()
            )

        input_series = series.astype(str).str.strip()
        temp_df = pd.DataFrame({lookup_col: input_series})
        merged = temp_df.merge(subset, on=lookup_col, how="left")

        unmapped = merged[return_col].isna().sum()
        if unmapped > 0:
            samples = (
                merged.loc[merged[return_col].isna(), lookup_col]
                .dropna()
                .unique()[:5]
            )
            print(
                f"     ⚠️  {unmapped} values had no match in lookup column "
                f"'{lookup_col}'. Sample: {list(samples)}"
            )

        return merged[return_col]

    def _strip_prefix(self, series: pd.Series) -> pd.Series:
        """
        Remove system/environment prefixes and normalise to uppercase.

        Algorithm
        ---------
        1. Strip the initial prefix token (any non-space, non-underscore chars
           before the first ``_``), regardless of case — e.g. ``Batch1``,
           ``Testing2``, ``PHASE``.
        2. Continue stripping subsequent lowercase/digit-only sub-tokens
           (e.g. ``phase``, ``1``) until the remainder starts with a token
           that contains uppercase letters (the real name).

        This correctly handles:
        - Single-level prefixes:  ``Batch1_HR`` → ``HR``
        - Multi-level prefixes:   ``Testing_phase_1_Belmont Village`` → ``BELMONT VILLAGE``
        - UPPER sub-tokens preserved: ``Batch1_MODIFIED_IT`` → ``MODIFIED_IT``
        - No underscore:           ``No Prefix Name`` → ``NO PREFIX NAME``

        Examples
        --------
        "Batch1.1_Amazing Grace Luxury Living, LLC." → "AMAZING GRACE LUXURY LIVING, LLC."
        "Testing_phase_1_Belmont Village"            → "BELMONT VILLAGE"
        "Batch1_MODIFIED_IT"                        → "MODIFIED_IT"
        "PHASE_1 LCS"                               → "1 LCS"
        """
        import re

        def _remove_prefix(val: str) -> str:
            if not val:
                return ""
            val = val.strip()
            # Step 1: strip the initial prefix token (e.g. Batch1, Testing2, PHASE)
            m = re.match(r"^[^\s_]+_(.+)$", val)
            if not m:
                return val
            val = m.group(1).strip()
            # Step 2: strip any remaining lowercase/digit-only sub-tokens (e.g. phase, 1)
            while True:
                m2 = re.match(r"^([a-z0-9]+)_(.+)$", val)
                if not m2:
                    break
                val = m2.group(2).strip()
            return val

        return series.astype(str).str.strip().apply(_remove_prefix).str.upper()
