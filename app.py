import io
import json
import time
from copy import deepcopy

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import streamlit as st

from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from google.oauth2 import service_account

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Lab Productivity Heatmaps",
    layout="wide",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Font
# ─────────────────────────────────────────────────────────────────────────────
def set_preferred_font():
    installed = {f.name for f in font_manager.fontManager.ttflist}
    for name in ("Palatino Linotype", "Palatino"):
        if name in installed:
            mpl.rcParams["font.family"] = name
            return
    mpl.rcParams["font.family"] = "serif"

set_preferred_font()

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_RESOURCES = {
    "Keck Core": [
        "Keck Abbott DI", "Keck Coagulation", "Keck Cobas",
        "Keck HEME Orders", "Keck IRIS", "Keck ISED", "Keck SmartLyte A",
        "Keck TEG 5000", "Keck Urinalysis", "USC Manual Coagulation Bench",
        "USC Manual Hematology Bench", "USC Manual Urinalysis Bench",
        "USC Serology Routine Bench",
    ],
    "Norris Core": [
        "NCH Coagulation", "NCH COBAS", "NCH HEME Orders", "NCH IRIS",
        "NCI Manual Chemistry Bench", "NCI Manual Hematology Bench",
        "NCI Stem Cell Bench", "NCH Cobas PRO A", "NCH Cobas PRO B",
        "NCH GEM 4000 H", "NCH GEM 4000 I",
    ],
    "Norris Specialty": [
        "NCH DS2 A", "NCH HydraSys", "NCH PFA 100", "NCH Tosoh G8",
        "NCI Manual Flow Bench", "NCI Manual Verify Now Bench",
    ],
}

VMAX = {"Keck Core": 50, "Norris Core": 30, "Norris Specialty": 20}

EXCLUDE_PROCS = {
    "Glomerular Filtration Rate Estimated",
    ".Diff Auto -",
    "Manual Diff-",
}

ALL_RESOURCES = sorted({r for rs in DEFAULT_RESOURCES.values() for r in rs})

def hour_label(h: int) -> str:
    hr12 = 12 if h % 12 == 0 else h % 12
    suffix = "AM" if h < 12 else "PM"
    return f"{hr12}{suffix}"

HOUR_LABELS  = {h: hour_label(h) for h in range(24)}
LABEL_TO_HOUR = {v: k for k, v in HOUR_LABELS.items()}

# ─────────────────────────────────────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────────────────────────────────────
if "resource_assignments" not in st.session_state:
    st.session_state.resource_assignments = deepcopy(DEFAULT_RESOURCES)
if "date_idx" not in st.session_state:
    st.session_state.date_idx = 0
if "last_map_type" not in st.session_state:
    st.session_state.last_map_type = None

# ─────────────────────────────────────────────────────────────────────────────
# Google Drive helpers
# ─────────────────────────────────────────────────────────────────────────────
DRIVE_CONFIGURED = (
    "gcp_service_account" in st.secrets and
    "google_drive" in st.secrets
)
GITHUB_CONFIGURED = "github" in st.secrets

def _get_creds():
    """Build and refresh service account credentials with full Drive scope."""
    from google.auth.transport.requests import Request as _GReq
    creds_info = dict(st.secrets["gcp_service_account"])
    # Normalise private key — Streamlit TOML may store \n as literal backslash-n
    pk = creds_info.get("private_key", "")
    if "\\n" in pk:
        pk = pk.replace("\\n", "\n")
    creds_info["private_key"] = pk
    creds = service_account.Credentials.from_service_account_info(
        creds_info,
        scopes=["https://www.googleapis.com/auth/drive"],
    )
    creds.refresh(_GReq())
    return creds


@st.cache_resource(show_spinner=False)
def get_drive_service():
    """Cached Drive API service (uses full drive scope)."""
    return build("drive", "v3", credentials=_get_creds(), cache_discovery=False)


def _get_authed_session():
    """Return a requests.Session pre-authorised with fresh Drive credentials.
    Never cached — always fresh for write operations.
    """
    from google.auth.transport.requests import AuthorizedSession
    return AuthorizedSession(_get_creds())


PARQUET_FILENAME = "lab_data.parquet"

# ─────────────────────────────────────────────────────────────────────────────
# GitHub storage helpers (used to read/write the master parquet file)
# ─────────────────────────────────────────────────────────────────────────────
def _gh_headers() -> dict:
    token = st.secrets["github"]["token"]
    return {"Authorization": f"Bearer {token}", "Accept": "application/vnd.github+json"}


def _gh_coords() -> tuple[str, str, str]:
    """Return (owner, repo, path) from secrets."""
    repo  = st.secrets["github"]["repo"]          # e.g. "michaelbask/lab-dashboard"
    path  = st.secrets["github"].get("data_path", "data/lab_data.parquet")
    owner, repo_name = repo.split("/", 1)
    return owner, repo_name, path


@st.cache_data(show_spinner=False, ttl=60)
def get_github_file_sha() -> str | None:
    """Return the current blob SHA of the parquet file, or None if it doesn't exist."""
    import requests as _r
    owner, repo, path = _gh_coords()
    resp = _r.get(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
        headers=_gh_headers(), timeout=15,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["sha"]


def read_parquet_from_github() -> bytes:
    """Download the parquet file bytes from GitHub."""
    import requests as _r, base64
    owner, repo, path = _gh_coords()
    resp = _r.get(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
        headers=_gh_headers(), timeout=30,
    )
    resp.raise_for_status()
    raw = base64.b64decode(resp.json()["content"])
    if len(raw) == 0:
        raise ValueError("Parquet file in GitHub is empty (0 bytes). Use Data Management to re-upload your data.")
    return raw


def write_parquet_to_github(parquet_bytes: bytes):
    """Create or update the parquet file in the GitHub repo."""
    import requests as _r, base64
    owner, repo, path = _gh_coords()

    # Safety check — never write empty or tiny files
    if len(parquet_bytes) < 100:
        raise ValueError(f"Refusing to write {len(parquet_bytes)}-byte file — data appears empty.")

    # Verify the repo is accessible before trying to write
    check = _r.get(
        f"https://api.github.com/repos/{owner}/{repo}",
        headers=_gh_headers(), timeout=15,
    )
    if check.status_code == 404:
        raise RuntimeError(
            f"Repo not found: '{owner}/{repo}'.\n\n"
            f"Check that the 'repo' value in your Streamlit secrets exactly matches "
            f"your GitHub repo name (it's case-sensitive).\n"
            f"It should look like: username/repo-name"
        )
    check.raise_for_status()

    sha = get_github_file_sha()
    # Strip any leading slash from path
    clean_path = path.lstrip("/")
    payload = {
        "message": "Update lab_data.parquet via dashboard",
        "content": base64.b64encode(parquet_bytes).decode(),
    }
    if sha:
        payload["sha"] = sha
    resp = _r.put(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{clean_path}",
        headers=_gh_headers(),
        json=payload,
        timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(
            f"GitHub write failed {resp.status_code}:\n"
            f"Repo: {owner}/{repo}\n"
            f"Path: {clean_path}\n"
            f"Response: {resp.text[:300]}"
        )
    get_github_file_sha.clear()


def get_parquet_file_meta(folder_id: str) -> dict | None:
    """Return metadata for lab_data.parquet in the Drive folder, or None if not found."""
    service = get_drive_service()
    result = service.files().list(
        q=f"'{folder_id}' in parents and name='{PARQUET_FILENAME}' and trashed=false",
        fields="files(id, name, modifiedTime)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = result.get("files", [])
    return files[0] if files else None


def download_drive_file(file_id: str, retries: int = 4) -> bytes:
    """Download a Drive file with automatic retry on SSL/network errors."""
    last_err = None
    for attempt in range(retries):
        try:
            service = get_drive_service()
            request = service.files().get_media(fileId=file_id, supportsAllDrives=True)
            buf = io.BytesIO()
            downloader = MediaIoBaseDownload(buf, request, chunksize=4 * 1024 * 1024)
            done = False
            while not done:
                _, done = downloader.next_chunk()
            buf.seek(0)
            return buf.read()
        except Exception as e:
            last_err = e
            wait = 2 ** attempt  # 1s, 2s, 4s, 8s
            time.sleep(wait)
    raise RuntimeError(f"Failed to download file after {retries} attempts: {last_err}")

def upload_parquet_to_drive(folder_id: str, parquet_bytes: bytes):
    """Upload or overwrite lab_data.parquet in Drive using AuthorizedSession.
    AuthorizedSession handles token injection automatically — no manual auth headers.
    """
    session = _get_authed_session()

    # Step 1 — check if file already exists
    list_resp = session.get(
        "https://www.googleapis.com/drive/v3/files",
        params={
            "q": f"'{folder_id}' in parents and name='{PARQUET_FILENAME}' and trashed=false",
            "fields": "files(id)",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        },
        timeout=30,
    )
    list_resp.raise_for_status()
    existing = list_resp.json().get("files", [])

    # Step 2 — build multipart body
    boundary = "lab_heatmap_upload_boundary"
    meta_json = json.dumps(
        {"name": PARQUET_FILENAME} if existing
        else {"name": PARQUET_FILENAME, "parents": [folder_id]}
    ).encode("utf-8")
    sep = b"\r\n"
    body = (
        b"--" + boundary.encode() + sep
        + b"Content-Type: application/json; charset=UTF-8" + sep + sep
        + meta_json + sep
        + b"--" + boundary.encode() + sep
        + b"Content-Type: application/octet-stream" + sep + sep
        + parquet_bytes + sep
        + b"--" + boundary.encode() + b"--" + sep
    )

    headers = {"Content-Type": f"multipart/related; boundary={boundary}"}

    # Step 3 — upload
    if existing:
        file_id = existing[0]["id"]
        resp = session.patch(
            f"https://www.googleapis.com/upload/drive/v3/files/{file_id}",
            params={"uploadType": "multipart", "supportsAllDrives": "true"},
            headers=headers,
            data=body,
            timeout=120,
        )
    else:
        resp = session.post(
            "https://www.googleapis.com/upload/drive/v3/files",
            params={"uploadType": "multipart", "supportsAllDrives": "true"},
            headers=headers,
            data=body,
            timeout=120,
        )

    if not resp.ok:
        raise RuntimeError(
            f"Upload failed {resp.status_code}: {resp.text[:500]}"
        )


# ─────────────────────────────────────────────────────────────────────────────
# Data loading and deduplication
# ─────────────────────────────────────────────────────────────────────────────
# Columns we actually need — ignore everything else for speed
REQUIRED_COLS = {
    "Performing Service Resource",
    "Order Procedure",
    "Date/Time - Complete",
    "Complete Volume",
}

def parse_single_file(file_bytes: bytes, filename: str = "") -> pd.DataFrame:
    """Parse CSV or Excel file into a clean DataFrame, loading only needed columns."""
    fname = filename.lower()

    if fname.endswith(".csv") or fname.endswith(".txt"):
        # Peek at header to find available columns
        header = pd.read_csv(io.BytesIO(file_bytes), nrows=0)
        header.columns = [c.strip() for c in header.columns]
        use_cols = [c for c in header.columns if c in REQUIRED_COLS]
        df = pd.read_csv(
            io.BytesIO(file_bytes),
            usecols=use_cols,
            low_memory=False,
        )
    else:
        # Excel — read first sheet, only needed columns
        header = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0, nrows=0)
        header.columns = [c.strip() if isinstance(c, str) else c for c in header.columns]
        use_cols = [c for c in header.columns if c in REQUIRED_COLS]
        df = pd.read_excel(
            io.BytesIO(file_bytes),
            sheet_name=0,
            usecols=use_cols,
            engine="openpyxl",
        )

    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    df["Performing Service Resource"] = df["Performing Service Resource"].astype(str).str.strip()
    df["Order Procedure"]             = df["Order Procedure"].astype(str).str.strip()

    # Remaps
    df.loc[
        (df["Order Procedure"] == "Kappa/Lambda Free Light Chains Panel") &
        (df["Performing Service Resource"] == "NCH COBAS"),
        "Performing Service Resource",
    ] = "NCI Manual Flow Bench"

    df.loc[
        (df["Order Procedure"] == "Manual Diff") &
        (df["Performing Service Resource"] == "Keck HEME Orders"),
        "Performing Service Resource",
    ] = "NCH HEME Orders"

    df["Date/Time - Complete"] = pd.to_datetime(df["Date/Time - Complete"], errors="coerce")
    df = df.dropna(subset=["Date/Time - Complete"])
    df["hour"]          = df["Date/Time - Complete"].dt.hour.astype(int)
    df["complete_date"] = df["Date/Time - Complete"].dt.date
    df["Complete Volume"] = (
        pd.to_numeric(df["Complete Volume"], errors="coerce").fillna(0).astype(float)
    )
    return df


def deduplicate_and_merge(frames: list[tuple[str, pd.DataFrame]]) -> pd.DataFrame:
    """
    Merge multiple DataFrames without double-counting overlapping rows.

    Strategy:
      1. Compute (min_dt, max_dt) for each file.
      2. Sort files by min_dt.
      3. For each consecutive pair, if file[i].max_dt > file[i+1].min_dt (overlap),
         truncate file[i] at file[i+1].min_dt so rows in the overlap window
         are taken exclusively from the later file.
      4. The last file is kept in full.

    Returns a single merged DataFrame and a summary list for display.
    """
    if not frames:
        return pd.DataFrame(), []

    # Compute ranges
    records = []
    for fname, df in frames:
        min_dt = df["Date/Time - Complete"].min()
        max_dt = df["Date/Time - Complete"].max()
        records.append({"fname": fname, "min_dt": min_dt, "max_dt": max_dt, "df": df})

    # Sort by start date
    records.sort(key=lambda r: r["min_dt"])

    summary = []
    result_dfs = []

    for i, rec in enumerate(records):
        df   = rec["df"].copy()
        cutoff = rec["max_dt"]  # default: keep everything

        if i + 1 < len(records):
            next_min = records[i + 1]["min_dt"]
            if next_min <= rec["max_dt"]:          # overlap detected
                cutoff = next_min
                df = df[df["Date/Time - Complete"] < cutoff]

        result_dfs.append(df)
        summary.append({
            "File": rec["fname"],
            "Data from":  rec["min_dt"].strftime("%Y-%m-%d %H:%M"),
            "Data to":    cutoff.strftime("%Y-%m-%d %H:%M"),
            "Rows kept":  len(df),
        })

    merged = pd.concat(result_dfs, ignore_index=True)
    return merged, summary


def merge_new_into_parquet(existing_df: pd.DataFrame, new_df: pd.DataFrame) -> tuple[pd.DataFrame, dict]:
    """
    Merge a newly uploaded file into the existing master DataFrame.
    Uses the same overlap-trimming logic as deduplicate_and_merge.
    Returns the merged DataFrame and a summary dict.
    """
    frames = [("existing", existing_df), ("new file", new_df)]
    merged, summary = deduplicate_and_merge(frames)
    stats = {
        "rows_before": len(existing_df),
        "rows_after":  len(merged),
        "rows_added":  len(merged) - len(existing_df),
        "new_date_range": (
            f"{new_df['complete_date'].min()} → {new_df['complete_date'].max()}"
        ),
    }
    return merged, stats


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# Auto-load from Google Drive (cached by the list of file IDs + modifiedTimes)
# ─────────────────────────────────────────────────────────────────────────────
def _parquet_bytes_to_df(raw: bytes) -> tuple[pd.DataFrame, list]:
    """Convert raw parquet bytes to DataFrame + summary."""
    df = pd.read_parquet(io.BytesIO(raw))
    if "complete_date" not in df.columns:
        df["complete_date"] = pd.to_datetime(df["Date/Time - Complete"]).dt.date
    if "hour" not in df.columns:
        df["hour"] = pd.to_datetime(df["Date/Time - Complete"]).dt.hour.astype(int)
    summary = [{
        "File": PARQUET_FILENAME,
        "Rows": len(df),
        "Date range": f"{df['complete_date'].min()} → {df['complete_date'].max()}",
    }]
    return df, summary


@st.cache_data(show_spinner="Loading data…", ttl=300)
def load_master_data(cache_key: str) -> tuple[pd.DataFrame, list]:
    """Load master parquet — from GitHub if configured, else from Drive.
    cache_key changes when the underlying file changes, busting the cache.
    """
    if GITHUB_CONFIGURED:
        raw = read_parquet_from_github()
    else:
        folder_id = st.secrets["google_drive"]["folder_id"]
        meta = get_parquet_file_meta(folder_id)
        if meta is None:
            raise FileNotFoundError("No data file found. Use Data Management to upload.")
        raw = download_drive_file(meta["id"])
    return _parquet_bytes_to_df(raw)


# ─────────────────────────────────────────────────────────────────────────────
# Manual upload (fallback when Drive is not configured)
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Processing uploaded files…")
def load_from_uploads(files_bytes: list[tuple[str, bytes]]) -> tuple[pd.DataFrame, list]:
    frames = [(name, parse_single_file(b, filename=name)) for name, b in files_bytes]
    return deduplicate_and_merge(frames)


# ─────────────────────────────────────────────────────────────────────────────
# Map filtering
# ─────────────────────────────────────────────────────────────────────────────
def filter_for_map(df: pd.DataFrame, map_type: str) -> pd.DataFrame:
    resources = st.session_state.resource_assignments[map_type]
    out = df[df["Performing Service Resource"].isin(resources)].copy()
    out = out[~out["Order Procedure"].isin(EXCLUDE_PROCS)].copy()
    top30 = (
        out.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(30).index.tolist()
    )
    return out[out["Order Procedure"].isin(top30)].copy()


# ─────────────────────────────────────────────────────────────────────────────
# Pivot + styling
# ─────────────────────────────────────────────────────────────────────────────
def build_pivot(df: pd.DataFrame, selected_date, hour_range):
    h_start, h_end = hour_range
    hours = list(range(h_start, h_end + 1))

    df_date      = df[df["complete_date"] == selected_date].copy()
    df_date_hour = df_date[df_date["hour"].isin(hours)].copy()

    if df_date_hour.empty:
        return None, None, df_date, hours

    pivot = (
        df_date_hour.pivot_table(
            index="Order Procedure",
            columns="hour",
            values="Complete Volume",
            aggfunc="sum",
            fill_value=0.0,
        ).reindex(columns=hours, fill_value=0.0)
    )
    pivot["Total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("Total", ascending=False)
    pivot.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in pivot.columns]
    return pivot, df_date_hour, df_date, hours


def style_pivot(pivot: pd.DataFrame, vmax: int):
    hour_cols = [c for c in pivot.columns if c != "Total"]
    return (
        pivot.style
        .background_gradient(cmap="viridis_r", vmin=0, vmax=vmax, subset=hour_cols)
        .format("{:.0f}")
        .set_properties(**{"text-align": "center"})
        .set_properties(subset=["Total"], **{
            "font-weight":  "bold",
            "font-size":    "13px",
            "border-left":  "2px solid #888",
        })
        .set_table_styles([
            {"selector": "th",           "props": [("text-align", "center"), ("font-size", "12px")]},
            {"selector": "th.row_heading","props": [("text-align", "left"),  ("min-width", "220px")]},
        ])
    )


# ─────────────────────────────────────────────────────────────────────────────
# PNG export
# ─────────────────────────────────────────────────────────────────────────────
def build_png(df_date_hour: pd.DataFrame, map_type: str, selected_date, hours: list) -> bytes:
    vmax    = VMAX[map_type]
    n_hours = len(hours)

    raw = (
        df_date_hour.pivot_table(
            index="Order Procedure",
            columns="hour",
            values="Complete Volume",
            aggfunc="sum",
            fill_value=0.0,
        ).reindex(columns=hours, fill_value=0.0)
    )
    raw["__total__"] = raw.sum(axis=1)
    raw = raw.sort_values("__total__", ascending=False)
    row_totals = raw["__total__"].values
    raw = raw.drop(columns=["__total__"])

    mat     = raw.to_numpy()
    ylabels = raw.index.tolist()
    x_labels = [HOUR_LABELS[h] for h in hours]

    fig_w = max(10, 0.6 * n_hours)
    fig_h = max(5,  0.35 * len(ylabels))
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)

    im = ax.imshow(mat, aspect="auto", cmap="viridis_r", vmin=0, vmax=vmax)
    date_str = pd.Timestamp(selected_date).strftime("%B %d, %Y")
    ax.set_title(f"{map_type} – Top 30 Procedures | {date_str}", fontsize=11, pad=10)
    ax.set_xticks(np.arange(n_hours) + 0.5)
    ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(np.arange(len(ylabels)))
    ax.set_yticklabels(ylabels, fontsize=8)

    cbar = fig.colorbar(im, ax=ax, fraction=0.02, pad=0.005)
    cbar.set_label("Completed volume in hour", fontsize=8)

    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            ax.text(j, i, f"{mat[i, j]:.0f}", ha="center", va="center",
                    fontsize=6.5, color="black")

    total_x = n_hours - 0.25
    ax.text(total_x, -0.75, "Total", ha="center", va="center",
            fontsize=8, fontweight="bold", color="black", transform=ax.transData)
    for i, total in enumerate(row_totals):
        ax.text(total_x, i, f"{total:.0f}", ha="center", va="center",
                fontsize=8, fontweight="bold", color="black", transform=ax.transData)
    ax.set_xlim(-0.5, n_hours + 0.25)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=250, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Controls")

    map_type = st.selectbox("Map type", list(DEFAULT_RESOURCES.keys()))

    if st.session_state.last_map_type != map_type:
        st.session_state.date_idx      = 0
        st.session_state.last_map_type = map_type

    st.markdown("---")

    # ── Data source ────────────────────────────────────────────────────────
    raw_df    = None
    merge_log = []

    # ── Handle pending reset ──────────────────────────────────────────────
    if st.session_state.pop("pending_reset", False):
        import requests as _rr, base64 as _b64
        try:
            if GITHUB_CONFIGURED:
                owner, repo, path = _gh_coords()
                sha = get_github_file_sha()
                if sha:
                    _rr.delete(
                        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
                        headers=_gh_headers(),
                        json={"message": "Reset master dataset", "sha": sha},
                        timeout=15,
                    )
                    get_github_file_sha.clear()
                    st.cache_data.clear()
            st.success("Master dataset cleared.")
        except Exception as re:
            st.error(f"Reset failed: {re}")

    # ── Handle pending upload (must run before any st.stop / cache calls) ──
    if "pending_upload" in st.session_state:
        pending = st.session_state.pop("pending_upload")
        with st.spinner(f"Processing {pending['name']}..."):
            new_df = parse_single_file(pending["bytes"], filename=pending["name"])
        st.caption(
            f"New file: {new_df['complete_date'].min()} → {new_df['complete_date'].max()} "
            f"({len(new_df):,} rows)"
        )
        # Load existing master
        try:
            if GITHUB_CONFIGURED:
                sha = get_github_file_sha()
                existing_df = pd.read_parquet(io.BytesIO(read_parquet_from_github())) if sha else pd.DataFrame()
            else:
                folder_id_e = st.secrets["google_drive"]["folder_id"]
                em = get_parquet_file_meta(folder_id_e)
                existing_df = pd.read_parquet(io.BytesIO(download_drive_file(em["id"]))) if em else pd.DataFrame()
        except Exception:
            existing_df = pd.DataFrame()

        with st.spinner("Merging and deduplicating..."):
            if existing_df.empty:
                merged_df   = new_df
                rows_before = 0
            else:
                merged_df, _stats = merge_new_into_parquet(existing_df, new_df)
                rows_before = _stats["rows_before"]

        with st.spinner("Saving master dataset..."):
            pq_bytes = df_to_parquet_bytes(merged_df)
            try:
                if GITHUB_CONFIGURED:
                    write_parquet_to_github(pq_bytes)
                else:
                    upload_parquet_to_drive(st.secrets["google_drive"]["folder_id"], pq_bytes)
                st.success(
                    f"Saved! {rows_before:,} → {len(merged_df):,} rows "
                    f"(+{len(merged_df)-rows_before:,} new)"
                )
            except Exception as ue:
                st.error(f"Save failed: {ue}")
        st.cache_data.clear()

    # ── Load master data ───────────────────────────────────────────────────
    if GITHUB_CONFIGURED or DRIVE_CONFIGURED:
        if GITHUB_CONFIGURED:
            st.markdown("**Data source:** GitHub")
        else:
            st.markdown("**Data source:** Google Drive")

        if st.button("Refresh data", use_container_width=True):
            st.cache_data.clear()
            st.rerun()

        _data_exists = False
        try:
            if GITHUB_CONFIGURED:
                _sha = get_github_file_sha()
                _data_exists = _sha is not None
                if _data_exists:
                    st.caption("Data file ready")
                    raw_df, merge_log = load_master_data(_sha)
                else:
                    st.warning("No data yet. Use **Data Management** below.")
            else:
                _folder_id = st.secrets["google_drive"]["folder_id"]
                _meta = get_parquet_file_meta(_folder_id)
                _data_exists = _meta is not None
                if _data_exists:
                    st.caption(f"Data last updated: {_meta['modifiedTime'][:10]}")
                    raw_df, merge_log = load_master_data(_meta["modifiedTime"])
                else:
                    st.warning("No data yet. Use **Data Management** below.")
        except Exception as e:
            st.error(f"Error loading data: {e}")

        # ── Data Management expander ───────────────────────────────────────
        st.markdown("---")
        with st.expander("Data Management", expanded=not _data_exists):
            st.markdown(
                "Upload a new XLSX export to add it to the master dataset. "
                "Overlapping date ranges are handled automatically."
            )
            admin_pw = st.secrets.get("admin_password", None)
            if admin_pw:
                entered_pw = st.text_input("Admin password", type="password", key="admin_pw")
                authorized = (entered_pw == admin_pw)
                if entered_pw and not authorized:
                    st.error("Incorrect password.")
            else:
                authorized = True

            if authorized:
                # Reset button — clears corrupt or unwanted master data
                if st.button("Reset master dataset", use_container_width=True):
                    st.session_state["pending_reset"] = True
                    st.rerun()

                st.markdown("---")
                new_file = st.file_uploader(
                    "Upload new XLSX file",
                    type=["xlsx", "xls"],
                    key="admin_upload",
                )
                if new_file is not None:
                    # Save bytes immediately — file_uploader loses state on button click rerun
                    if "staged_bytes" not in st.session_state or st.session_state.get("staged_name") != new_file.name:
                        st.session_state["staged_bytes"] = new_file.read()
                        st.session_state["staged_name"]  = new_file.name
                    st.caption(f"Ready: {new_file.name}")
                    if st.button("Process and add to master dataset",
                                 type="primary", use_container_width=True):
                        st.session_state["pending_upload"] = {
                            "bytes": st.session_state["staged_bytes"],
                            "name":  st.session_state["staged_name"],
                        }
                        st.session_state.pop("staged_bytes", None)
                        st.session_state.pop("staged_name",  None)
                        st.rerun()

    else:
        st.markdown("**Data source:** File upload")
        st.caption("Configure Google Drive or GitHub in secrets to enable auto-loading.")
        uploaded_files = st.file_uploader(
            "Upload Excel file(s)", type=["xlsx", "xls"],
            accept_multiple_files=True,
        )
        if uploaded_files:
            files_bytes = [(f.name, f.read()) for f in uploaded_files]
            raw_df, merge_log = load_from_uploads(files_bytes)

    st.markdown("---")

    if raw_df is not None and not raw_df.empty:
        filtered_df     = filter_for_map(raw_df, map_type)
        available_dates = sorted(filtered_df["complete_date"].unique())
        date_labels     = [pd.Timestamp(d).strftime("%B %d, %Y") for d in available_dates]

        if not available_dates:
            st.warning("No data found for this map type.")
            st.stop()

        st.session_state.date_idx = max(
            0, min(st.session_state.date_idx, len(available_dates) - 1)
        )

        # Date navigation
        st.markdown("**Date**")
        c1, c2 = st.columns(2)
        with c1:
            if st.button("◀ Prev", use_container_width=True):
                st.session_state.date_idx = max(0, st.session_state.date_idx - 1)
                st.rerun()
        with c2:
            if st.button("Next ▶", use_container_width=True):
                st.session_state.date_idx = min(
                    len(available_dates) - 1, st.session_state.date_idx + 1
                )
                st.rerun()

        sel_label = st.selectbox(
            "Select date", date_labels,
            index=st.session_state.date_idx,
            label_visibility="collapsed",
        )
        st.session_state.date_idx = date_labels.index(sel_label)
        selected_date = available_dates[st.session_state.date_idx]
        st.caption(f"Day {st.session_state.date_idx + 1} of {len(available_dates)}")

        # Hour range
        st.markdown("---")
        st.markdown("**Hour range**")
        hour_range = st.slider("Hours", 0, 23, (0, 23), label_visibility="collapsed")
        h0, h1 = hour_range

        def fmt_h(h):
            hr12 = 12 if h % 12 == 0 else h % 12
            s = "AM" if h < 12 else "PM"
            return f"{hr12}:00 {s}"

        st.caption(f"{fmt_h(h0)} → {fmt_h(h1)}")

        # Resource allocation
        st.markdown("---")
        with st.expander("Resource Allocation", expanded=False):
            st.markdown(
                "Reassign instruments between maps. "
                "Each resource should appear in only one map."
            )
            new_assignments = {}
            for mt in DEFAULT_RESOURCES:
                new_assignments[mt] = st.multiselect(
                    mt, options=ALL_RESOURCES,
                    default=st.session_state.resource_assignments.get(mt, []),
                    key=f"res_{mt}",
                )
            flat  = [r for rs in new_assignments.values() for r in rs]
            dupes = sorted({r for r in flat if flat.count(r) > 1})
            if dupes:
                st.warning(f"In multiple maps: {', '.join(dupes)}")

            ca, cr = st.columns(2)
            with ca:
                if st.button("Apply", use_container_width=True, type="primary"):
                    st.session_state.resource_assignments = new_assignments
                    st.cache_data.clear()
                    st.rerun()
            with cr:
                if st.button("Reset defaults", use_container_width=True):
                    st.session_state.resource_assignments = deepcopy(DEFAULT_RESOURCES)
                    st.cache_data.clear()
                    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Process pending upload (runs in main panel where st.status is fully visible)
# ─────────────────────────────────────────────────────────────────────────────
if "pending_upload" in st.session_state and st.session_state["pending_upload"]:
    upload_info = st.session_state["pending_upload"]
    st.markdown("## Processing new data file...")

    with st.status("Processing...", expanded=True) as status:
        st.write(f"Parsing **{upload_info['name']}**...")
        try:
            new_df = parse_single_file(upload_info["bytes"], filename=upload_info["name"])
            st.write(f"Parsed {len(new_df):,} rows "
                     f"({new_df['complete_date'].min()} → {new_df['complete_date'].max()})")

            st.write("Loading existing master dataset...")
            try:
                if GITHUB_CONFIGURED:
                    sha = get_github_file_sha()
                    if sha:
                        existing_bytes = read_parquet_from_github()
                        existing_df = pd.read_parquet(io.BytesIO(existing_bytes))
                        st.write(f"Existing dataset: {len(existing_df):,} rows")
                    else:
                        existing_df = pd.DataFrame()
                        st.write("No existing dataset — creating new master.")
                else:
                    folder_id_up = st.secrets["google_drive"]["folder_id"]
                    existing_meta = get_parquet_file_meta(folder_id_up)
                    if existing_meta:
                        existing_bytes = download_drive_file(existing_meta["id"])
                        existing_df = pd.read_parquet(io.BytesIO(existing_bytes))
                        st.write(f"Existing dataset: {len(existing_df):,} rows")
                    else:
                        existing_df = pd.DataFrame()
                        st.write("No existing dataset — creating new master.")
            except Exception:
                existing_df = pd.DataFrame()
                st.write("No existing dataset — creating new master.")

            st.write("Merging and deduplicating...")
            if existing_df.empty:
                merged_df   = new_df
                rows_before = 0
                rows_added  = len(new_df)
            else:
                merged_df, _stats = merge_new_into_parquet(existing_df, new_df)
                rows_before = _stats["rows_before"]
                rows_added  = _stats["rows_added"]
            st.write(f"Merged: {rows_before:,} → {len(merged_df):,} rows (+{rows_added:,})")

            st.write("Saving to GitHub...")
            parquet_bytes_up = df_to_parquet_bytes(merged_df)
            size_mb = len(parquet_bytes_up) / 1024 / 1024
            st.write(f"File size: {size_mb:.2f} MB")
            if GITHUB_CONFIGURED:
                write_parquet_to_github(parquet_bytes_up)
            else:
                folder_id_up = st.secrets["google_drive"]["folder_id"]
                upload_parquet_to_drive(folder_id_up, parquet_bytes_up)

            status.update(label=f"Done! Added {rows_added:,} rows to master dataset.",
                          state="complete")
            st.session_state.pop("pending_upload", None)
            st.cache_data.clear()
            st.rerun()

        except Exception as proc_err:
            status.update(label="Processing failed.", state="error")
            st.error(str(proc_err))
            st.session_state.pop("pending_upload", None)
    st.stop()

# ─────────────────────────────────────────────────────────────────────────────
# Main panel
# ─────────────────────────────────────────────────────────────────────────────
if raw_df is None or raw_df.empty:
    st.markdown("## Upload a file or configure Google Drive to get started")
    st.info(
        "This dashboard displays completed lab order volumes as an interactive "
        "heatmap. Use the sidebar to load your data."
    )
    st.stop()

pivot, df_date_hour, df_date, hours = build_pivot(filtered_df, selected_date, hour_range)

if pivot is None:
    st.warning("No data found for this date / hour range combination.")
    st.stop()

# Header
date_str = pd.Timestamp(selected_date).strftime("%B %d, %Y")
st.markdown(f"## {map_type}  ·  {date_str}")

# Metrics
total_vol     = int(pivot["Total"].sum())
top_proc      = pivot["Total"].idxmax()
hour_cols_only = [c for c in pivot.columns if c != "Total"]
peak_hour     = pivot[hour_cols_only].sum().idxmax()
num_procs     = len(pivot)

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Volume",     f"{total_vol:,}")
m2.metric("Top Procedure",    top_proc)
m3.metric("Peak Hour",        peak_hour)
m4.metric("Procedures Shown", num_procs)

st.markdown("---")

# Heatmap table
st.markdown("### Completed volume per hour")
st.caption(
    f"Color scale: yellow = low  →  dark purple = ≥ {VMAX[map_type]}/hour.  "
    f"**Total** = full-day sum per procedure."
)
table_height = min(80 + 35 * len(pivot), 900)
st.dataframe(style_pivot(pivot, VMAX[map_type]), use_container_width=True, height=table_height)

# Download
png_bytes   = build_png(df_date_hour, map_type, selected_date, hours)
file_prefix = map_type.replace(" ", "_")
date_tag    = pd.Timestamp(selected_date).strftime("%Y-%m-%d")
st.download_button(
    label="Download as PNG",
    data=png_bytes,
    file_name=f"{file_prefix}_Top30_{date_tag}.png",
    mime="image/png",
)

st.markdown("---")

# Hourly bar chart
with st.expander("Hourly volume bar chart", expanded=False):
    hourly = pivot[hour_cols_only].sum().reset_index()
    hourly.columns = ["Hour", "Total Volume"]
    st.bar_chart(hourly.set_index("Hour"), height=220)

# File merge log
if merge_log:
    with st.expander("Data file details — overlap handling", expanded=False):
        st.markdown(
            "The table below shows how data files were combined. "
            "When two files cover overlapping time ranges, the earlier file "
            "is trimmed at the start of the later file to prevent double-counting."
        )
        st.dataframe(pd.DataFrame(merge_log), use_container_width=True)

# Cell drill-down
with st.expander("Drill into a cell — individual completion times", expanded=False):
    st.markdown(
        "Select a **procedure** and **hour** to see every individual completion "
        "event recorded in that cell."
    )
    dc1, dc2 = st.columns(2)
    with dc1:
        sel_proc       = st.selectbox("Procedure", pivot.index.tolist(), key="drill_proc")
    with dc2:
        sel_hour_label = st.selectbox("Hour", hour_cols_only, key="drill_hour")

    sel_hour_int = LABEL_TO_HOUR[sel_hour_label]
    detail = df_date[
        (df_date["Order Procedure"] == sel_proc) &
        (df_date["hour"] == sel_hour_int)
    ].copy().sort_values("Date/Time - Complete")

    show_cols = {k: v for k, v in {
        "Date/Time - Complete":        "Completed At",
        "Performing Service Resource": "Resource",
        "Complete Volume":             "Volume",
    }.items() if k in detail.columns}

    detail_display = (
        detail[list(show_cols.keys())]
        .rename(columns=show_cols)
        .reset_index(drop=True)
    )
    if "Completed At" in detail_display.columns:
        detail_display["Completed At"] = pd.to_datetime(
            detail_display["Completed At"]
        ).dt.strftime("%Y-%m-%d  %H:%M:%S")

    if detail_display.empty:
        st.info(f"No completions for **{sel_proc}** during **{sel_hour_label}** on {date_str}.")
    else:
        cell_vol = int(detail_display["Volume"].sum()) if "Volume" in detail_display.columns else len(detail_display)
        st.markdown(
            f"**{len(detail_display)} event(s)** · *{sel_proc}* · "
            f"**{sel_hour_label}** · Total volume: **{cell_vol}**"
        )
        drill_height = min(80 + 35 * len(detail_display), 500)
        st.dataframe(detail_display, use_container_width=True, height=drill_height)
