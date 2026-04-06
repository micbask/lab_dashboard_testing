"""
parsing.py — File ingestion pipeline.

Handles:
  - SpreadsheetML XML (.xls) detection and parsing
  - Legacy binary XLS via xlrd
  - Standard XLSX via openpyxl
  - CSV/TXT files
  - Multi-sheet workbook reading (all sheets concatenated)
  - Forward-fill of accession clusters
  - Expanded 14-column set
  - Date/time format normalization
  - Resource remaps and procedure name cleaning
"""

import io
import re
import xml.etree.ElementTree as ET

import pandas as pd

from config import (
    ALL_SOURCE_COLUMNS,
    DATETIME_COLUMNS,
    FORWARD_FILL_COLS,
    RESOURCE_REMAPS,
)


# ═════════════════════════════════════════════════════════════════════════════
# PROCEDURE NAME CLEANING
# ═════════════════════════════════════════════════════════════════════════════

def clean_procedure_names(df: pd.DataFrame) -> pd.DataFrame:
    """Normalise known procedure-name encoding artefacts (\\xa0 variants)."""
    if "Order Procedure" not in df.columns:
        return df
    df["Order Procedure"] = df["Order Procedure"].str.replace(
        "Complete Blood Count With Auto\xa0 Differen",
        "Complete Blood Count With Auto  Differen",
        regex=False,
    )
    df["Order Procedure"] = df["Order Procedure"].str.replace(
        "Complete Blood Count With Auto\xa0Differen",
        "Complete Blood Count With Auto  Differen",
        regex=False,
    )
    return df


# ═════════════════════════════════════════════════════════════════════════════
# SPREADSHEETML XML PARSER
# ═════════════════════════════════════════════════════════════════════════════

_SS_NS = "urn:schemas-microsoft-com:office:spreadsheet"
_SS_NSMAP = {"ss": _SS_NS}


def _is_spreadsheetml(file_bytes: bytes) -> bool:
    """Detect whether file_bytes is a SpreadsheetML XML file."""
    header = file_bytes[:500]
    try:
        header_str = header.decode("utf-8", errors="ignore")
    except Exception:
        return False
    return (
        header_str.lstrip().startswith("<?xml")
        and "urn:schemas-microsoft-com:office:spreadsheet" in header_str
    )


def _parse_spreadsheetml(file_bytes: bytes) -> list[pd.DataFrame]:
    """Parse SpreadsheetML XML into a list of DataFrames (one per worksheet)."""
    root = ET.fromstring(file_bytes)

    sheets = []
    for worksheet in root.findall("ss:Worksheet", _SS_NSMAP):
        table = worksheet.find("ss:Table", _SS_NSMAP)
        if table is None:
            continue

        rows_data = []
        for row_el in table.findall("ss:Row", _SS_NSMAP):
            cells = []
            col_idx = 0
            for cell_el in row_el.findall("ss:Cell", _SS_NSMAP):
                # Handle ss:Index attribute (1-based column skip)
                idx_attr = cell_el.get(f"{{{_SS_NS}}}Index")
                if idx_attr:
                    target_idx = int(idx_attr) - 1
                    while col_idx < target_idx:
                        cells.append("")
                        col_idx += 1

                data_el = cell_el.find("ss:Data", _SS_NSMAP)
                cells.append(data_el.text if data_el is not None and data_el.text else "")
                col_idx += 1
            rows_data.append(cells)

        if len(rows_data) < 2:
            continue

        headers = rows_data[0]
        # Normalize column count
        max_cols = max(len(r) for r in rows_data)
        headers = headers + [""] * (max_cols - len(headers))
        data_rows = []
        for r in rows_data[1:]:
            padded = r + [""] * (max_cols - len(r))
            data_rows.append(padded)

        df = pd.DataFrame(data_rows, columns=headers)
        # Strip whitespace from column names
        df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
        sheets.append(df)

    return sheets


# ═════════════════════════════════════════════════════════════════════════════
# BINARY XLS DETECTION
# ═════════════════════════════════════════════════════════════════════════════

def _is_binary_xls(file_bytes: bytes) -> bool:
    """Detect legacy binary XLS (BIFF/Compound Document) by magic bytes."""
    return file_bytes[:8].startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1")


# ═════════════════════════════════════════════════════════════════════════════
# DATE/TIME NORMALIZATION
# ═════════════════════════════════════════════════════════════════════════════

# Common date formats from the source system
_DATE_FORMATS = [
    "%b %d, %Y %I:%M:%S %p",   # "Mar 31, 2026 2:14:29 PM"
    "%B %d, %Y %I:%M:%S %p",   # "March 31, 2026 2:14:29 PM"
    "%m/%d/%Y %I:%M:%S %p",    # "03/31/2026 2:14:29 PM"
    "%m/%d/%Y %H:%M:%S",       # "03/31/2026 14:14:29"
    "%Y-%m-%d %H:%M:%S",       # "2026-03-31 14:14:29"
    "%m/%d/%Y %H:%M",          # "03/31/2026 14:14"
]


def _normalize_datetime_column(series: pd.Series) -> pd.Series:
    """Parse a datetime column, trying known formats then falling back to pandas."""
    if series.dtype == "datetime64[ns]":
        return series

    # Try pd.to_datetime first (handles most formats)
    result = pd.to_datetime(series, errors="coerce", format="mixed")

    # If many NaTs, try each explicit format
    nat_count = result.isna().sum()
    orig_nat_count = series.isna().sum() + (series == "").sum()
    if nat_count > orig_nat_count + len(series) * 0.1:
        for fmt in _DATE_FORMATS:
            try:
                attempt = pd.to_datetime(series, format=fmt, errors="coerce")
                if attempt.notna().sum() > result.notna().sum():
                    result = attempt
            except Exception:
                continue

    return result


def normalize_datetimes(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize all known datetime columns to datetime64[ns]."""
    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = _normalize_datetime_column(df[col])
    return df


# ═════════════════════════════════════════════════════════════════════════════
# FORWARD-FILL ACCESSION CLUSTERS
# ═════════════════════════════════════════════════════════════════════════════

def forward_fill_accession_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """Forward-fill Accession Nbr, Patient Location, Facility within clusters.

    Only fills genuinely blank/empty cells — does not overwrite existing values.
    """
    for col in FORWARD_FILL_COLS:
        if col in df.columns:
            # Replace empty strings with NaN so ffill works, then fill
            df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)
            df[col] = df[col].ffill()
    return df


# ═════════════════════════════════════════════════════════════════════════════
# COLUMN FILTERING
# ═════════════════════════════════════════════════════════════════════════════

def select_available_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Keep only the recognized source columns that exist in the DataFrame."""
    available = [c for c in ALL_SOURCE_COLUMNS if c in df.columns]
    return df[available].copy()


# ═════════════════════════════════════════════════════════════════════════════
# RESOURCE REMAPS
# ═════════════════════════════════════════════════════════════════════════════

def apply_resource_remaps(df: pd.DataFrame) -> pd.DataFrame:
    """Apply RESOURCE_REMAPS to reassign resources based on procedure+resource pairs."""
    if "Order Procedure" not in df.columns or "Performing Service Resource" not in df.columns:
        return df
    for (proc, old_res), new_res in RESOURCE_REMAPS.items():
        mask = (df["Order Procedure"] == proc) & (df["Performing Service Resource"] == old_res)
        df.loc[mask, "Performing Service Resource"] = new_res
    return df


# ═════════════════════════════════════════════════════════════════════════════
# DERIVED COLUMNS
# ═════════════════════════════════════════════════════════════════════════════

def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Add hour, complete_date, inlab_hour, inlab_date derived columns."""
    if "Date/Time - Complete" in df.columns:
        df["Date/Time - Complete"] = pd.to_datetime(
            df["Date/Time - Complete"], errors="coerce"
        )
        df = df.dropna(subset=["Date/Time - Complete"])
        df["hour"] = df["Date/Time - Complete"].dt.hour.astype(int)
        df["complete_date"] = df["Date/Time - Complete"].dt.date

    if "Date/Time - In Lab" in df.columns:
        df["Date/Time - In Lab"] = pd.to_datetime(
            df["Date/Time - In Lab"], errors="coerce"
        )
        df["inlab_hour"] = df["Date/Time - In Lab"].dt.hour.astype("Int64")
        df["inlab_date"] = df["Date/Time - In Lab"].dt.date
    else:
        df["Date/Time - In Lab"] = pd.NaT
        df["inlab_hour"] = pd.array([pd.NA] * len(df), dtype="Int64")
        df["inlab_date"] = None

    if "Complete Volume" in df.columns:
        df["Complete Volume"] = (
            pd.to_numeric(df["Complete Volume"], errors="coerce").fillna(0).astype(float)
        )

    return df


# ═════════════════════════════════════════════════════════════════════════════
# READ SHEETS FROM ANY FORMAT
# ═════════════════════════════════════════════════════════════════════════════

def _read_all_sheets(file_bytes: bytes, filename: str) -> list[pd.DataFrame]:
    """Read all sheets from a file, handling SpreadsheetML, binary XLS, XLSX, and CSV."""
    fname = filename.lower()

    if fname.endswith(".csv") or fname.endswith(".txt"):
        df = pd.read_csv(io.BytesIO(file_bytes), low_memory=False)
        df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
        return [df]

    # Check for SpreadsheetML XML
    if _is_spreadsheetml(file_bytes):
        sheets = _parse_spreadsheetml(file_bytes)
        if sheets:
            return sheets
        # Fall through if parsing produced no sheets

    # Check for binary XLS
    if _is_binary_xls(file_bytes):
        import xlrd
        wb = xlrd.open_workbook(file_contents=file_bytes)
        sheets = []
        for sheet in wb.sheets():
            if sheet.nrows < 2:
                continue
            headers = [sheet.cell_value(0, c) for c in range(sheet.ncols)]
            data = []
            for r in range(1, sheet.nrows):
                row = [sheet.cell_value(r, c) for c in range(sheet.ncols)]
                data.append(row)
            df = pd.DataFrame(data, columns=headers)
            df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
            sheets.append(df)
        return sheets if sheets else []

    # Standard XLSX via openpyxl
    xls = pd.ExcelFile(io.BytesIO(file_bytes), engine="openpyxl")
    sheets = []
    for sheet_name in xls.sheet_names:
        df = pd.read_excel(xls, sheet_name=sheet_name)
        df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
        if not df.empty:
            sheets.append(df)
    return sheets


# ═════════════════════════════════════════════════════════════════════════════
# MAIN PARSE FUNCTION
# ═════════════════════════════════════════════════════════════════════════════

def parse_single_file(file_bytes: bytes, filename: str = "") -> pd.DataFrame:
    """Parse an uploaded file into a clean, analysis-ready DataFrame.

    Pipeline:
      1. Read all sheets (SpreadsheetML / XLS / XLSX / CSV)
      2. Forward-fill accession clusters per sheet
      3. Select available columns from the expanded 14-column set
      4. Concatenate all sheets
      5. Normalize datetime columns
      6. Clean procedure names
      7. Apply resource remaps
      8. Add derived columns (hour, complete_date, etc.)
    """
    raw_sheets = _read_all_sheets(file_bytes, filename)

    if not raw_sheets:
        return pd.DataFrame()

    processed_sheets = []
    for sheet_df in raw_sheets:
        # Forward-fill accession clusters before column filtering
        sheet_df = forward_fill_accession_clusters(sheet_df)
        # Keep only recognized columns
        sheet_df = select_available_columns(sheet_df)
        if not sheet_df.empty:
            processed_sheets.append(sheet_df)

    if not processed_sheets:
        return pd.DataFrame()

    df = pd.concat(processed_sheets, ignore_index=True)

    # Strip string columns
    if "Performing Service Resource" in df.columns:
        df["Performing Service Resource"] = df["Performing Service Resource"].astype(str).str.strip()
    if "Order Procedure" in df.columns:
        df["Order Procedure"] = df["Order Procedure"].astype(str).str.strip()

    # Normalize datetimes
    df = normalize_datetimes(df)

    # Clean procedure names
    df = clean_procedure_names(df)

    # Apply resource remaps
    df = apply_resource_remaps(df)

    # Add derived columns
    df = add_derived_columns(df)

    return df


# ═════════════════════════════════════════════════════════════════════════════
# DEDUPLICATION & MERGING
# ═════════════════════════════════════════════════════════════════════════════

def deduplicate_and_merge(
    frames: list[tuple[str, pd.DataFrame]],
) -> tuple[pd.DataFrame, list[dict]]:
    """Merge DataFrames from multiple files, trimming overlapping time windows."""
    if not frames:
        return pd.DataFrame(), []

    records = sorted(
        [
            {
                "fname":  name,
                "min_dt": df["Date/Time - Complete"].min(),
                "max_dt": df["Date/Time - Complete"].max(),
                "df":     df,
            }
            for name, df in frames
        ],
        key=lambda r: r["min_dt"],
    )

    summary    = []
    result_dfs = []
    for i, rec in enumerate(records):
        df     = rec["df"].copy()
        cutoff = rec["max_dt"]
        if i + 1 < len(records):
            next_min = records[i + 1]["min_dt"]
            if next_min <= cutoff:
                cutoff = next_min
                df = df[df["Date/Time - Complete"] < cutoff]
        result_dfs.append(df)
        summary.append({
            "File":      rec["fname"],
            "Data from": rec["min_dt"].strftime("%Y-%m-%d %H:%M"),
            "Data to":   cutoff.strftime("%Y-%m-%d %H:%M"),
            "Rows kept": len(df),
        })

    return pd.concat(result_dfs, ignore_index=True), summary
