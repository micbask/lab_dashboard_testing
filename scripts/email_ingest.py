#!/usr/bin/env python3
"""
email_ingest.py — Standalone Apple Mail → Parquet ingest.

Runs locally on a Mac via launchd.  This script is fully standalone:
no imports from a parent repo, no Streamlit.  Drop the scripts/ folder
anywhere on a Mac, populate scripts/.env, install
`pip install -r requirements.txt`, and schedule the plist.

Pipeline per run:
  1. Run fetch_attachment.applescript, which tells Apple Mail to find
     messages from the last 24 hours in the inbox of
     michael.bask@med.usc.edu whose subject contains
     "Lab Order Department Volume Analysis- All Labs Daily Report",
     save every .xls/.xlsx attachment into the OneDrive drop folder,
     and mark each source message as read.
  2. Scan the drop folder for new files.
  3. Parse each file (XLS / XLSX / SpreadsheetML-XML / CSV) into a
     clean DataFrame using the same pipeline the Streamlit app uses,
     inlined here (no repo imports).
  4. Write the resulting rows into the partitioned Parquet store on
     GitHub via the Contents API — each affected monthly partition
     lands as its own commit on the configured branch, exactly the
     same way the dashboard ingests uploads.
  5. Move the processed file into `processed/<YYYY-MM-DD>/` so the
     next run does not re-ingest it.

ENVIRONMENT (scripts/.env — see .env.example):
    GITHUB_TOKEN         PAT with contents:write on the target repo
    GITHUB_REPOSITORY    "owner/name", e.g. micbask/lab_dashboard_testing
    DROP_FOLDER          optional override of the OneDrive drop folder
    LOG_FILE             optional log path override
"""

from __future__ import annotations

import base64
import io
import json
import logging
import os
import re
import shutil
import subprocess
import sys
import time
import xml.etree.ElementTree as ET
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests


# ═════════════════════════════════════════════════════════════════════════════
# CONSTANTS (inlined from config.py)
# ═════════════════════════════════════════════════════════════════════════════

ALL_SOURCE_COLUMNS: list[str] = [
    "Facility",
    "Patient Location",
    "Collection Priority",
    "Accession Nbr - Formatted",
    "Order Procedure",
    "Performing Service Resource",
    "Date/Time - Order",
    "Date/Time - Drawn",
    "Date/Time - In Lab",
    "Date/Time - Complete",
    "Drawn Tech",
    "Drawn Tech - Position",
    "Order Status",
    "Complete Volume",
]

FORWARD_FILL_COLS: list[str] = [
    "Accession Nbr - Formatted",
    "Patient Location",
    "Facility",
]

DATETIME_COLUMNS: list[str] = [
    "Date/Time - Order",
    "Date/Time - Drawn",
    "Date/Time - In Lab",
    "Date/Time - Complete",
]

RESOURCE_REMAPS: dict[tuple[str, str], str] = {
    ("Kappa/Lambda Free Light Chains Panel", "NCH COBAS"): "NCI Manual Flow Bench",
    ("Manual Diff", "Keck HEME Orders"): "NCH HEME Orders",
}

# GitHub partitioned storage layout (matches the Streamlit dashboard).
PARTITION_DIR = "data/partitions"
PARTITION_INDEX_PATH = "data/partition_index.json"


# ═════════════════════════════════════════════════════════════════════════════
# PATHS & .env LOADING
# ═════════════════════════════════════════════════════════════════════════════

SCRIPT_DIR = Path(__file__).resolve().parent
ENV_PATH = SCRIPT_DIR / ".env"
APPLESCRIPT_PATH = SCRIPT_DIR / "fetch_attachment.applescript"

# Same path hardcoded inside fetch_attachment.applescript. Change both
# together, or set DROP_FOLDER in .env to override the Python side.
DEFAULT_DROP_FOLDER = Path(
    "/Users/michaelbask/Library/CloudStorage/"
    "OneDrive-KeckMedicineofUSC/Work/Productivity Heat Maps/xls_ingest"
)


def _load_env_file(path: Path) -> None:
    """Minimal .env loader — KEY=VALUE lines, # comments, optional quotes."""
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


_load_env_file(ENV_PATH)


# ═════════════════════════════════════════════════════════════════════════════
# LOGGING
# ═════════════════════════════════════════════════════════════════════════════

_default_log = Path.home() / "Library" / "Logs" / "lab-dashboard-ingest.log"
LOG_FILE = Path(os.environ.get("LOG_FILE", str(_default_log)))
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("email_ingest")


# ═════════════════════════════════════════════════════════════════════════════
# PARSING PIPELINE (inlined from parsing.py)
# ═════════════════════════════════════════════════════════════════════════════

_SS_NS = "urn:schemas-microsoft-com:office:spreadsheet"
_SS_NSMAP = {"ss": _SS_NS}

_DATE_FORMATS = [
    "%b %d, %Y %I:%M:%S %p",
    "%B %d, %Y %I:%M:%S %p",
    "%m/%d/%Y %I:%M:%S %p",
    "%m/%d/%Y %H:%M:%S",
    "%Y-%m-%d %H:%M:%S",
    "%m/%d/%Y %H:%M",
]


def _is_spreadsheetml(file_bytes: bytes) -> bool:
    """Detect Microsoft SpreadsheetML 2003 XML (exported as .xls)."""
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
    sheets: list[pd.DataFrame] = []
    for worksheet in root.findall("ss:Worksheet", _SS_NSMAP):
        table = worksheet.find("ss:Table", _SS_NSMAP)
        if table is None:
            continue

        rows_data: list[list[str]] = []
        for row_el in table.findall("ss:Row", _SS_NSMAP):
            cells: list[str] = []
            col_idx = 0
            for cell_el in row_el.findall("ss:Cell", _SS_NSMAP):
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
        max_cols = max(len(r) for r in rows_data)
        headers = headers + [""] * (max_cols - len(headers))
        data_rows = [r + [""] * (max_cols - len(r)) for r in rows_data[1:]]

        df = pd.DataFrame(data_rows, columns=headers)
        df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
        sheets.append(df)

    return sheets


def _is_binary_xls(file_bytes: bytes) -> bool:
    """Detect legacy binary XLS (OLE Compound Document) by its magic bytes."""
    return file_bytes[:8].startswith(b"\xd0\xcf\x11\xe0\xa1\xb1\x1a\xe1")


def _normalize_datetime_column(series: pd.Series) -> pd.Series:
    """Parse a column to datetime64[ns], trying multiple known formats."""
    if series.dtype == "datetime64[ns]":
        return series

    result = pd.to_datetime(series, errors="coerce", format="mixed")

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
    for col in DATETIME_COLUMNS:
        if col in df.columns:
            df[col] = _normalize_datetime_column(df[col])
    return df


def forward_fill_accession_clusters(df: pd.DataFrame) -> pd.DataFrame:
    """Fill blank cells in cluster columns from the nearest non-blank row above."""
    for col in FORWARD_FILL_COLS:
        if col in df.columns:
            df[col] = df[col].replace(r"^\s*$", pd.NA, regex=True)
            df[col] = df[col].ffill()
    return df


def select_available_columns(df: pd.DataFrame) -> pd.DataFrame:
    available = [c for c in ALL_SOURCE_COLUMNS if c in df.columns]
    return df[available].copy()


def clean_procedure_names(df: pd.DataFrame) -> pd.DataFrame:
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


def apply_resource_remaps(df: pd.DataFrame) -> pd.DataFrame:
    if "Order Procedure" not in df.columns or "Performing Service Resource" not in df.columns:
        return df
    for (proc, old_res), new_res in RESOURCE_REMAPS.items():
        mask = (df["Order Procedure"] == proc) & (df["Performing Service Resource"] == old_res)
        df.loc[mask, "Performing Service Resource"] = new_res
    return df


def add_derived_columns(df: pd.DataFrame) -> pd.DataFrame:
    if "Date/Time - Complete" in df.columns:
        df["Date/Time - Complete"] = pd.to_datetime(df["Date/Time - Complete"], errors="coerce")
        df = df.dropna(subset=["Date/Time - Complete"])
        df["hour"] = df["Date/Time - Complete"].dt.hour.astype(int)
        df["complete_date"] = df["Date/Time - Complete"].dt.date

    if "Date/Time - In Lab" in df.columns:
        df["Date/Time - In Lab"] = pd.to_datetime(df["Date/Time - In Lab"], errors="coerce")
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


def _read_all_sheets(file_bytes: bytes, filename: str) -> list[pd.DataFrame]:
    fname = filename.lower()

    if fname.endswith(".csv") or fname.endswith(".txt"):
        df = pd.read_csv(io.BytesIO(file_bytes), low_memory=False)
        df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
        return [df]

    if _is_spreadsheetml(file_bytes):
        sheets = _parse_spreadsheetml(file_bytes)
        if sheets:
            return sheets

    if _is_binary_xls(file_bytes):
        import xlrd  # deferred import so missing xlrd only blocks legacy XLS

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


def parse_single_file(file_bytes: bytes, filename: str = "") -> pd.DataFrame:
    """Full parse pipeline: identical to the dashboard's parsing.parse_single_file()."""
    raw_sheets = _read_all_sheets(file_bytes, filename)
    if not raw_sheets:
        return pd.DataFrame()

    processed_sheets = []
    for sheet_df in raw_sheets:
        sheet_df = forward_fill_accession_clusters(sheet_df)
        sheet_df = select_available_columns(sheet_df)
        if not sheet_df.empty:
            processed_sheets.append(sheet_df)

    if not processed_sheets:
        return pd.DataFrame()

    df = pd.concat(processed_sheets, ignore_index=True)

    if "Performing Service Resource" in df.columns:
        df["Performing Service Resource"] = df["Performing Service Resource"].astype(str).str.strip()
    if "Order Procedure" in df.columns:
        df["Order Procedure"] = df["Order Procedure"].astype(str).str.strip()

    df = normalize_datetimes(df)
    df = clean_procedure_names(df)
    df = apply_resource_remaps(df)
    df = add_derived_columns(df)
    return df


# ═════════════════════════════════════════════════════════════════════════════
# GITHUB PARTITIONED STORAGE (inlined from storage.py, streamlit-free)
# ═════════════════════════════════════════════════════════════════════════════

# Module-level cache of the partition index so we hit GitHub exactly once
# per run, even if ingest_new_data is called multiple times.
_INDEX_CACHE: Optional[dict] = None


def _github_token() -> str:
    tok = os.environ.get("GITHUB_TOKEN", "").strip()
    if not tok:
        raise SystemExit(
            "Missing GITHUB_TOKEN — populate scripts/.env (see .env.example)."
        )
    return tok


def _github_repo() -> tuple[str, str]:
    repo = os.environ.get("GITHUB_REPOSITORY", "").strip()
    if not repo or "/" not in repo:
        raise SystemExit(
            "Missing or invalid GITHUB_REPOSITORY — expected 'owner/name'."
        )
    owner, name = repo.split("/", 1)
    return owner, name


def _gh_headers() -> dict:
    return {
        "Authorization": f"Bearer {_github_token()}",
        "Accept": "application/vnd.github+json",
    }


def _gh_get_file(path: str, retries: int = 3) -> tuple[Optional[bytes], Optional[str]]:
    """Download a file from GitHub. Returns (content_bytes, sha) or (None, None)."""
    owner, repo = _github_repo()
    last_err: Optional[Exception] = None
    for attempt in range(retries):
        try:
            resp = requests.get(
                f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
                headers=_gh_headers(), timeout=30,
            )
            if resp.status_code == 404:
                return None, None
            resp.raise_for_status()
            data = resp.json()
            sha = data.get("sha")
            content_str = data.get("content", "").strip()
            if content_str:
                raw = base64.b64decode(content_str)
            elif data.get("download_url"):
                dl = requests.get(data["download_url"], headers=_gh_headers(), timeout=60)
                dl.raise_for_status()
                raw = dl.content
            else:
                raw = b""
            return raw, sha
        except Exception as exc:
            last_err = exc
            if attempt < retries - 1:
                time.sleep(2 ** attempt)
    raise RuntimeError(
        f"GitHub read failed for {path} after {retries} attempts: {last_err}"
    )


def _gh_put_file(path: str, content_bytes: bytes, message: str,
                 sha: Optional[str] = None) -> str:
    """Create or update a file on GitHub. Returns the new blob SHA."""
    owner, repo = _github_repo()
    payload: dict = {
        "message": message,
        "content": base64.b64encode(content_bytes).decode(),
    }
    if sha:
        payload["sha"] = sha
    resp = requests.put(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
        headers=_gh_headers(), json=payload, timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(
            f"GitHub write failed {resp.status_code} for {path}:\n{resp.text[:300]}"
        )
    return resp.json()["content"]["sha"]


def _partition_key(dt: date) -> str:
    return f"{dt.year:04d}-{dt.month:02d}"


def _partition_path(key: str) -> str:
    return f"{PARTITION_DIR}/{key}.parquet"


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return buf.getvalue()


def _parquet_bytes_to_df(raw: bytes) -> pd.DataFrame:
    df = pd.read_parquet(io.BytesIO(raw))
    if "complete_date" not in df.columns and "Date/Time - Complete" in df.columns:
        df["complete_date"] = pd.to_datetime(df["Date/Time - Complete"]).dt.date
    if "hour" not in df.columns and "Date/Time - Complete" in df.columns:
        df["hour"] = pd.to_datetime(df["Date/Time - Complete"]).dt.hour.astype(int)
    if "Date/Time - In Lab" in df.columns:
        if "inlab_date" not in df.columns:
            df["inlab_date"] = pd.to_datetime(
                df["Date/Time - In Lab"], errors="coerce"
            ).dt.date
        if "inlab_hour" not in df.columns:
            df["inlab_hour"] = pd.to_datetime(
                df["Date/Time - In Lab"], errors="coerce"
            ).dt.hour.astype("Int64")
    return df


def _load_partition_index() -> dict:
    """Load the index (cached in memory after first call)."""
    global _INDEX_CACHE
    if _INDEX_CACHE is not None:
        return _INDEX_CACHE
    raw, _ = _gh_get_file(PARTITION_INDEX_PATH)
    if raw is None:
        _INDEX_CACHE = {}
        return _INDEX_CACHE
    try:
        _INDEX_CACHE = json.loads(raw.decode("utf-8"))
    except Exception:
        _INDEX_CACHE = {}
    return _INDEX_CACHE


def _save_partition_index(index: dict) -> None:
    global _INDEX_CACHE
    content = json.dumps(index, indent=2, default=str).encode("utf-8")
    _, sha = _gh_get_file(PARTITION_INDEX_PATH)
    _gh_put_file(PARTITION_INDEX_PATH, content, "Update partition index", sha=sha)
    _INDEX_CACHE = index


def _read_partition(key: str) -> Optional[pd.DataFrame]:
    index = _load_partition_index()
    if key not in index:
        return None
    raw, _ = _gh_get_file(_partition_path(key))
    if raw is None:
        return None
    df = _parquet_bytes_to_df(raw)
    return df if not df.empty else None


def ingest_new_data(new_df: pd.DataFrame) -> dict:
    """Merge new_df into the partitioned store — only touches affected months.

    Identical behaviour to storage.ingest_new_data() in the dashboard repo.
    """
    if new_df.empty:
        return {"rows_added": 0}

    if "complete_date" not in new_df.columns:
        new_df["complete_date"] = pd.to_datetime(new_df["Date/Time - Complete"]).dt.date

    new_df["_partition_key"] = new_df["complete_date"].apply(_partition_key)
    index = _load_partition_index()
    total_before = sum(p["rows"] for p in index.values())

    for key, new_group in new_df.groupby("_partition_key"):
        new_group = new_group.drop(columns=["_partition_key"])
        existing_df = _read_partition(key)

        if existing_df is not None and not existing_df.empty:
            new_min = new_group["Date/Time - Complete"].min()
            new_max = new_group["Date/Time - Complete"].max()
            existing_trimmed = existing_df[
                (existing_df["Date/Time - Complete"] < new_min)
                | (existing_df["Date/Time - Complete"] > new_max)
            ]
            merged = pd.concat([existing_trimmed, new_group], ignore_index=True)
            merged = merged.sort_values("Date/Time - Complete").reset_index(drop=True)
        else:
            merged = new_group.reset_index(drop=True)

        pq_bytes = df_to_parquet_bytes(merged)
        existing_sha = index.get(key, {}).get("sha")

        new_sha = _gh_put_file(
            _partition_path(key), pq_bytes,
            f"Update partition {key} ({len(merged):,} rows)",
            sha=existing_sha,
        )

        index[key] = {
            "sha": new_sha,
            "rows": len(merged),
            "min_date": str(merged["complete_date"].min()),
            "max_date": str(merged["complete_date"].max()),
            "size_bytes": len(pq_bytes),
        }

    if "_partition_key" in new_df.columns:
        new_df.drop(columns=["_partition_key"], inplace=True)

    _save_partition_index(index)

    total_after = sum(p["rows"] for p in index.values())
    return {
        "rows_before": total_before,
        "rows_after": total_after,
        "rows_added": total_after - total_before,
    }


def trigger_retrain_forecast() -> None:
    """Fire a repository_dispatch to retrain forecasts after a successful ingest.

    Uses the same GITHUB_TOKEN / GITHUB_REPOSITORY loaded from scripts/.env.
    Non-fatal — any error is logged and swallowed so a dispatch failure never
    breaks the ingest run.
    """
    try:
        owner, repo = _github_repo()
        resp = requests.post(
            f"https://api.github.com/repos/{owner}/{repo}/dispatches",
            headers={
                **_gh_headers(),
                "Content-Type": "application/json",
            },
            json={
                "event_type": "retrain-forecast",
                "client_payload": {
                    "source": "email_ingest",
                    "triggered_at": datetime.utcnow().isoformat() + "Z",
                },
            },
            timeout=30,
        )
        if resp.status_code in (200, 201, 204):
            log.info("Dispatched retrain-forecast event to %s/%s", owner, repo)
        else:
            log.warning(
                "retrain-forecast dispatch failed: %s %s",
                resp.status_code, resp.text[:300],
            )
    except Exception as exc:
        log.warning("retrain-forecast dispatch raised: %s", exc)


# ═════════════════════════════════════════════════════════════════════════════
# APPLE MAIL FETCH
# ═════════════════════════════════════════════════════════════════════════════

def _run_applescript() -> None:
    """Run fetch_attachment.applescript via /usr/bin/osascript."""
    if not APPLESCRIPT_PATH.exists():
        raise SystemExit(f"AppleScript not found: {APPLESCRIPT_PATH}")

    log.info("Running AppleScript: %s", APPLESCRIPT_PATH)
    try:
        proc = subprocess.run(
            ["/usr/bin/osascript", str(APPLESCRIPT_PATH)],
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        log.error("AppleScript timed out after 300s")
        return
    except FileNotFoundError:
        log.error("/usr/bin/osascript not found — is this running on macOS?")
        return

    for stream_name, stream in (("stdout", proc.stdout), ("stderr", proc.stderr)):
        for line in (stream or "").splitlines():
            line = line.strip()
            if not line:
                continue
            if line.startswith("LINE: "):
                log.info("applescript: %s", line[6:])
            else:
                log.info("applescript[%s]: %s", stream_name, line)

    if proc.returncode != 0:
        log.error("AppleScript exited with status %s", proc.returncode)


# ═════════════════════════════════════════════════════════════════════════════
# DROP-FOLDER INGEST
# ═════════════════════════════════════════════════════════════════════════════

_XLS_SUFFIXES = (".xls", ".xlsx", ".csv", ".txt")


def _drop_folder() -> Path:
    override = os.environ.get("DROP_FOLDER", "").strip()
    return Path(override) if override else DEFAULT_DROP_FOLDER


def _iter_incoming_files(folder: Path):
    if not folder.exists():
        log.warning("Drop folder does not exist: %s", folder)
        return
    for entry in sorted(folder.iterdir()):
        if entry.is_file() and entry.suffix.lower() in _XLS_SUFFIXES:
            yield entry


def _process_file(path: Path, processed_dir: Path) -> int:
    try:
        content = path.read_bytes()
    except Exception as exc:
        log.exception("  • failed to read %s: %s", path.name, exc)
        return 0

    if not content:
        log.warning("  • %s is empty, skipping", path.name)
        return 0

    log.info("  • parsing %s (%d bytes)", path.name, len(content))
    df = parse_single_file(content, filename=path.name)
    if df is None or df.empty:
        log.warning("  • parse_single_file produced 0 rows for %s", path.name)
        return 0

    log.info("  • ingesting %d rows into partitioned store...", len(df))
    summary = ingest_new_data(df)
    added = int(summary.get("rows_added", 0))
    log.info("  • ingest summary: %s", summary)

    day_dir = processed_dir / datetime.now().strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    dest = day_dir / path.name
    if dest.exists():
        stem = dest.stem
        suffix = dest.suffix
        i = 1
        while (day_dir / f"{stem}.{i}{suffix}").exists():
            i += 1
        dest = day_dir / f"{stem}.{i}{suffix}"
    shutil.move(str(path), str(dest))
    log.info("  • moved to %s", dest)

    return added


def main() -> int:
    log.info("=" * 64)
    log.info("Starting email ingest run (standalone)")

    # Step 1: Apple Mail drops today's attachments into the folder.
    _run_applescript()

    # Step 2: ingest whatever is sitting in the drop folder.
    drop = _drop_folder()
    drop.mkdir(parents=True, exist_ok=True)
    processed = drop / "processed"
    log.info("Scanning drop folder: %s", drop)

    matched = 0
    total_added = 0
    for f in _iter_incoming_files(drop):
        matched += 1
        log.info("File #%d: %s", matched, f.name)
        try:
            total_added += _process_file(f, processed)
        except Exception as exc:
            log.exception("Failed to process %s: %s", f.name, exc)

    if matched == 0:
        log.info("No incoming files to process.")
    else:
        log.info(
            "Ingest run complete: %d file(s), %d row(s) added.",
            matched, total_added,
        )

    if total_added > 0:
        trigger_retrain_forecast()

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
