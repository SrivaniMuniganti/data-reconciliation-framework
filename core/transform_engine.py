"""
core/transform_engine.py
-------------------------
Applies schema-mapping rules to raw DataFrames, producing a normalised
representation ready for row-by-row reconciliation.

The engine harmonises two datasets that may differ in:
- Column names        (origin → destination remapping)
- Data types          (all values normalised to strings)
- Value encodings     (e.g. boolean flags, ID translations, prefixed names)

The transformation is driven entirely by the ``SchemaParser`` configuration,
keeping business logic separate from data processing code.
"""

import pandas as pd
from core.rule_engine import RuleEngine


class TransformEngine:
    """
    Applies column mappings and transformation rules from a ``SchemaParser``
    to a raw DataFrame representing either the origin or destination dataset.

    Parameters
    ----------
    schema_parser : SchemaParser
        Parsed mapping configuration providing column maps, rules, and keys.
    lookup_df : pd.DataFrame, optional
        ID lookup table forwarded to ``RuleEngine`` for ``idmap`` rules.
    """

    def __init__(self, schema_parser, lookup_df: pd.DataFrame | None = None):
        self._column_map = schema_parser.get_mappings()      # {src_col: tgt_col}
        self._rule_map = schema_parser.get_rules()           # {src_col: rule metadata}
        self._key_columns = schema_parser.get_key_columns()  # [tgt key cols]
        self._rule_engine = RuleEngine(lookup_df)

        print("     🔧 TransformEngine initialised")
        print(f"     📋 Column mappings: {len(self._column_map)}")
        print(f"     🔑 Key columns: {self._key_columns}")

    # -------------------------------------------------------------------------
    # Public API
    # -------------------------------------------------------------------------

    def transform(self, df: pd.DataFrame, side: str) -> pd.DataFrame:
        """
        Produce a normalised DataFrame by applying all mapping rules.

        Parameters
        ----------
        df : pd.DataFrame
            Raw input data — either the origin or destination dataset.
        side : str
            ``"origin"`` (source) or ``"destination"`` (target).
            Controls which input column name to read from and which rules to apply.

        Returns
        -------
        pd.DataFrame
            Transformed data with destination column names.

        Raises
        ------
        ValueError
            If ``side`` is not ``"origin"`` or ``"destination"``.
        RuntimeError
            If a key column is absent from the input DataFrame.
        """
        side = side.lower()
        if side not in ("origin", "destination"):
            raise ValueError("side must be 'origin' or 'destination'")

        # Normalise column names: lowercase + strip whitespace
        df = df.copy()
        df.columns = df.columns.astype(str).str.strip().str.lower()

        output = pd.DataFrame(index=df.index)

        for src_col, tgt_col in self._column_map.items():
            rule_cfg = self._rule_map.get(src_col, {})
            rule = rule_cfg.get("rule", "")
            params = rule_cfg.get("params", {})
            apply_on = rule_cfg.get("apply_on", "BOTH").upper()

            # Determine which column name to read from the raw DataFrame
            input_col = src_col.lower() if side == "origin" else tgt_col.lower()

            # ── Skip rule if not applicable for this side ───────────────────
            if apply_on not in ("BOTH", side.upper().replace("ORIGIN", "SOURCE").replace("DESTINATION", "TARGET")):
                # Map side labels for apply_on compatibility
                side_label = "SOURCE" if side == "origin" else "TARGET"
                if apply_on not in ("BOTH", side_label):
                    if input_col in df.columns:
                        # Normalise case for strip_prefix columns even when rule is skipped
                        if rule == "strip_prefix" and side == "origin":
                            output[tgt_col] = df[input_col].astype(str).str.strip().str.upper()
                        else:
                            output[tgt_col] = df[input_col].copy()
                    else:
                        print(f"     ⚠️  Column '{input_col}' not found in {side} data — inserting nulls")
                        output[tgt_col] = pd.Series([None] * len(df))
                    continue

            # ── Extract input Series ────────────────────────────────────────
            if input_col not in df.columns:
                is_key = rule_cfg.get("is_key", False)
                if is_key:
                    raise RuntimeError(
                        f"Key column '{input_col}' is absent from the {side} dataset. "
                        f"Available columns: {list(df.columns)}"
                    )
                print(f"     ⚠️  Column '{input_col}' not found in {side} data — inserting nulls")
                series = pd.Series([None] * len(df))
            else:
                series = df[input_col].copy()

            # ── Apply transformation ────────────────────────────────────────
            try:
                output[tgt_col] = self._rule_engine.apply(rule, series, params)
            except Exception as exc:
                print(f"     ❌ Rule '{rule}' failed on column '{input_col}': {exc}")
                raise

        print(f"     ✅ Transformation complete: {len(output)} rows, {len(output.columns)} columns")
        return output
