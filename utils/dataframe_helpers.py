"""
utils/dataframe_helpers.py
---------------------------
Shared DataFrame utilities used across the DataSync Audit framework.
"""

import os
from datetime import datetime

import pandas as pd


def normalise_headers(df: pd.DataFrame) -> pd.DataFrame:
    """
    Clean DataFrame column headers to prevent mapping and join key mismatches.

    Normalisation steps
    -------------------
    1. Cast all header values to ``str``.
    2. Strip leading and trailing whitespace.
    3. Remove zero-width space characters (U+200B).
    4. Remove byte-order mark characters (U+FEFF).

    Parameters
    ----------
    df : pd.DataFrame
        Input DataFrame whose headers need cleaning.

    Returns
    -------
    pd.DataFrame
        Same DataFrame with cleaned column headers (in-place modification).

    Examples
    --------
    >>> df = pd.DataFrame(columns=[" CustomerID ", "Name\\u200b", "\\ufeffEmail"])
    >>> normalise_headers(df).columns.tolist()
    ['CustomerID', 'Name', 'Email']
    """
    df.columns = (
        df.columns
        .astype(str)
        .str.strip()
        .str.replace("\u200b", "", regex=False)   # zero-width space
        .str.replace("\ufeff", "", regex=False)   # byte-order mark
    )
    return df


def write_dataframe(
    df: pd.DataFrame,
    output_dir: str,
    filename: str,
    file_format: str = "csv",
    include_timestamp: bool = True,
) -> str:
    """
    Persist a DataFrame to disk in CSV or Excel format.

    Parameters
    ----------
    df               : pd.DataFrame   Data to write.
    output_dir       : str            Destination directory (created if absent).
    filename         : str            Base filename without extension.
    file_format      : str            ``"csv"`` or ``"excel"`` / ``"xlsx"``.
    include_timestamp: bool           Append a ``_YYYYMMDD_HHMMSS`` suffix.

    Returns
    -------
    str
        Absolute path to the written file.

    Raises
    ------
    ValueError
        If ``file_format`` is not ``"csv"`` or ``"excel"``/``"xlsx"``.
    """
    os.makedirs(output_dir, exist_ok=True)

    suffix = ""
    if include_timestamp:
        suffix = f"_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    fmt = file_format.lower()

    if fmt == "csv":
        path = os.path.join(output_dir, f"{filename}{suffix}.csv")
        df.to_csv(path, index=False)

    elif fmt in ("xlsx", "excel"):
        path = os.path.join(output_dir, f"{filename}{suffix}.xlsx")
        df.to_excel(path, index=False, engine="openpyxl")

    else:
        raise ValueError(
            f"Unsupported file_format '{file_format}'. Use 'csv' or 'excel'."
        )

    print(f"     💾 Written {len(df)} rows → {path}")
    return path
