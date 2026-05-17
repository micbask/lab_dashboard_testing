"""
storage.py — Partitioned Parquet storage on GitHub with DuckDB query layer.

Data architecture for 5M+ rows:
  - Data is partitioned by month: data/partitions/YYYY-MM.parquet
  - A lightweight JSON index (data/partition_index.json) tracks each
    partition's date range, row count, and GitHub blob SHA.
  - Uploads only read/write the affected monthly partitions.
  - Reads only load the partition(s) needed for the current view.
  - DuckDB filters rows at read time — never loads full dataset into RAM.
  - Legacy single-file (data/lab_data.parquet) is auto-migrated on
    first access.

CRITICAL DESIGN RULE: The dashboard query path NEVER loads more than one
month of data into memory at a time.  For daily view, only one partition
is read.  For monthly view, only the month's partition.  The full dataset
is NEVER materialized except during forecast training (which streams
partition-by-partition).
"""

import base64
import io
import json
import time
from datetime import date, timedelta
from typing import Optional

import pandas as pd
import requests
import streamlit as st

from config import (
    ALL_SOURCE_COLUMNS,
    PARTITION_DIR,
    PARTITION_INDEX_PATH,
    LEGACY_PARQUET_PATH,
)


# Columns that load_filtered_data's result always carries when there's
# data — the source columns plus the derived date/hour helpers added
# by _parquet_bytes_to_df. The empty-result helper below uses this
# list so callers can do `df["complete_date"] == x` on a no-data
# frame without a KeyError. The pre-Batch-2 DuckDB pipeline preserved
# the schema implicitly via fetchdf(); after the partition-streaming
# refactor we have to manufacture it on the empty paths ourselves.
_RESULT_COLUMNS: list[str] = list(ALL_SOURCE_COLUMNS) + [
    "complete_date", "hour",
    "inlab_date",    "inlab_hour",
    "drawn_date",    "drawn_hour",
]


def _empty_result_frame() -> pd.DataFrame:
    """Empty DataFrame with the columns load_filtered_data returns when
    it finds data. Use this on every no-data early-return path so
    downstream consumers can rely on column presence.
    """
    return pd.DataFrame(columns=_RESULT_COLUMNS)


# ═════════════════════════════════════════════════════════════════════════════
# GITHUB API HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _gh_headers() -> dict:
    return {
        "Authorization": f"Bearer {st.secrets['github']['token']}",
        "Accept": "application/vnd.github+json",
    }


def _gh_repo() -> tuple[str, str]:
    """Return (owner, repo_name)."""
    repo = st.secrets["github"]["repo"]
    owner, repo_name = repo.split("/", 1)
    return owner, repo_name


def _gh_get_file(path: str, retries: int = 3) -> tuple[Optional[bytes], Optional[str]]:
    """Download a file from GitHub. Returns (content_bytes, sha) or (None, None)."""
    owner, repo = _gh_repo()
    last_err = None
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
    raise RuntimeError(f"GitHub read failed for {path} after {retries} attempts: {last_err}")


def _gh_put_file(path: str, content_bytes: bytes, message: str, sha: Optional[str] = None) -> str:
    """Create or update a file on GitHub. Returns new SHA."""
    owner, repo = _gh_repo()
    payload = {
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


def _gh_delete_file(path: str, sha: str, message: str) -> None:
    """Delete a file on GitHub."""
    owner, repo = _gh_repo()
    requests.delete(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
        headers=_gh_headers(),
        json={"message": message, "sha": sha},
        timeout=15,
    )


# ═════════════════════════════════════════════════════════════════════════════
# PARTITION INDEX — cached in session state to avoid repeated GitHub reads
# ═════════════════════════════════════════════════════════════════════════════

def _load_partition_index() -> dict:
    """Load the partition index, using session-state cache.

    The index is a tiny JSON file (<1KB) — cache it in session state so
    we only hit GitHub once per session (or after explicit invalidation).

    Returns a DEEP COPY of the cached dict so callers can safely mutate
    their copy before deciding whether to persist it via
    `_save_partition_index`. The previous implementation returned the
    cached reference directly, which meant mid-write failures (a
    `_gh_put_file` exception after the in-memory mutation) left
    session_state and the remote out of sync.
    """
    import copy as _copy
    _ss = st.session_state
    if "_partition_index" in _ss:
        return _copy.deepcopy(_ss["_partition_index"])

    raw, _ = _gh_get_file(PARTITION_INDEX_PATH)
    if raw is None:
        _ss["_partition_index"] = {}
        return {}
    try:
        index = json.loads(raw.decode("utf-8"))
    except Exception:
        index = {}
    _ss["_partition_index"] = index
    return _copy.deepcopy(index)


def _invalidate_index_cache() -> None:
    """Clear the cached partition index so the next call re-reads from GitHub."""
    st.session_state.pop("_partition_index", None)


def _save_partition_index(index: dict) -> None:
    """Save the partition index to GitHub and update session-state cache."""
    content = json.dumps(index, indent=2, default=str).encode("utf-8")
    _, sha = _gh_get_file(PARTITION_INDEX_PATH)
    _gh_put_file(
        PARTITION_INDEX_PATH, content,
        "Update partition index",
        sha=sha,
    )
    st.session_state["_partition_index"] = index


def _partition_key(dt: date) -> str:
    """Return the YYYY-MM partition key for a date."""
    return f"{dt.year:04d}-{dt.month:02d}"


def _partition_path(key: str) -> str:
    """Return the GitHub path for a partition file."""
    return f"{PARTITION_DIR}/{key}.parquet"


# ═════════════════════════════════════════════════════════════════════════════
# PARQUET HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise a DataFrame to Snappy-compressed Parquet."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return buf.getvalue()


def _parquet_bytes_to_df(raw: bytes) -> pd.DataFrame:
    """Deserialise Parquet bytes, ensuring derived helper columns exist."""
    df = pd.read_parquet(io.BytesIO(raw))
    if "complete_date" not in df.columns and "Date/Time - Complete" in df.columns:
        df["complete_date"] = pd.to_datetime(df["Date/Time - Complete"]).dt.date
    if "hour" not in df.columns and "Date/Time - Complete" in df.columns:
        df["hour"] = pd.to_datetime(df["Date/Time - Complete"]).dt.hour.astype(int)
    if "Date/Time - In Lab" in df.columns:
        if "inlab_date" not in df.columns:
            df["inlab_date"] = pd.to_datetime(df["Date/Time - In Lab"], errors="coerce").dt.date
        if "inlab_hour" not in df.columns:
            df["inlab_hour"] = pd.to_datetime(
                df["Date/Time - In Lab"], errors="coerce"
            ).dt.hour.astype("Int64")
    if "Date/Time - Drawn" in df.columns:
        if "drawn_date" not in df.columns:
            df["drawn_date"] = pd.to_datetime(df["Date/Time - Drawn"], errors="coerce").dt.date
        if "drawn_hour" not in df.columns:
            df["drawn_hour"] = pd.to_datetime(
                df["Date/Time - Drawn"], errors="coerce"
            ).dt.hour.astype("Int64")
    return df


# ═════════════════════════════════════════════════════════════════════════════
# PARTITION READ — cached per-partition by SHA
# ═════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False, ttl=600)
def _read_partition_cached(key: str, sha: str) -> pd.DataFrame:
    """Read a single partition from GitHub, cached by (key, sha).

    The sha parameter ensures the cache is busted when the partition is
    rewritten.  TTL=600s provides a safety net.
    """
    raw, _ = _gh_get_file(_partition_path(key))
    if raw is None:
        return pd.DataFrame()
    return _parquet_bytes_to_df(raw)


def _read_partition(key: str) -> Optional[pd.DataFrame]:
    """Read a partition, using per-SHA caching."""
    index = _load_partition_index()
    meta = index.get(key)
    if meta is None:
        return None
    sha = meta.get("sha", "")
    df = _read_partition_cached(key, sha)
    return df if not df.empty else None


# ═════════════════════════════════════════════════════════════════════════════
# LEGACY MIGRATION
# ═════════════════════════════════════════════════════════════════════════════

def _migrate_legacy_parquet() -> bool:
    """Migrate the legacy single-file parquet to partitioned storage."""
    raw, legacy_sha = _gh_get_file(LEGACY_PARQUET_PATH)
    if raw is None or len(raw) < 100:
        return False

    df = _parquet_bytes_to_df(raw)
    if df.empty:
        return False

    _write_partitions_from_df(df)

    if legacy_sha:
        try:
            _gh_delete_file(LEGACY_PARQUET_PATH, legacy_sha,
                            "Remove legacy monolithic parquet (migrated to partitions)")
        except Exception:
            pass

    return True


# Maximum draw -> complete latency the pre-analytics partition pruner
# assumes. Used as a slack term on the lower side of the complete-date
# range when scoping queries by drawn_date — see load_filtered_data.
# 30 days is generous for lab phlebotomy draws (typical latency is
# hours to a few days); raising it just makes the pruner read one or
# two extra partitions per query.
_DRAWN_LAG_BUFFER = timedelta(days=30)


def _write_partitions_from_df(df: pd.DataFrame) -> dict:
    """Split a DataFrame by month and write each partition to GitHub."""
    if "complete_date" not in df.columns:
        if "Date/Time - Complete" in df.columns:
            df["complete_date"] = pd.to_datetime(df["Date/Time - Complete"]).dt.date
        else:
            return {}

    df["_partition_key"] = df["complete_date"].apply(_partition_key)
    index = _load_partition_index()

    for key, group in df.groupby("_partition_key"):
        group = group.drop(columns=["_partition_key"])
        pq_bytes = df_to_parquet_bytes(group)
        existing_sha = index.get(key, {}).get("sha")

        new_sha = _gh_put_file(
            _partition_path(key), pq_bytes,
            f"Write partition {key} ({len(group):,} rows)",
            sha=existing_sha,
        )

        index[key] = {
            "sha": new_sha,
            "rows": len(group),
            "min_date": str(group["complete_date"].min()),
            "max_date": str(group["complete_date"].max()),
            "size_bytes": len(pq_bytes),
        }

    if "_partition_key" in df.columns:
        df.drop(columns=["_partition_key"], inplace=True)

    _save_partition_index(index)
    return index


# ═════════════════════════════════════════════════════════════════════════════
# PUBLIC STORAGE API
# ═════════════════════════════════════════════════════════════════════════════

def storage_is_configured() -> bool:
    """Check if GitHub storage is configured."""
    return "github" in st.secrets


def get_data_summary() -> dict:
    """Return summary from the partition index — NO data loading.

    This is O(1) — reads a small JSON file, not any Parquet data.
    Returns {total_rows, min_date, max_date, partitions} or empty dict.
    """
    index = _load_partition_index()
    if not index:
        return {"total_rows": 0, "partitions": 0}

    total_rows = sum(p["rows"] for p in index.values())
    all_min = min(p["min_date"] for p in index.values())
    all_max = max(p["max_date"] for p in index.values())
    return {
        "total_rows": total_rows,
        "min_date": all_min,
        "max_date": all_max,
        "partitions": len(index),
    }


@st.cache_data(show_spinner=False, ttl=300)
def load_filtered_data(
    start_date: date,
    end_date: date,
    resources: tuple[str, ...],
    exclude_procs: tuple[str, ...],
    index_hash: str = "",
    date_basis: str = "complete",
) -> pd.DataFrame:
    """Load ONLY the data needed for a specific view.

    This is the primary read function for the dashboard.  It:
      1. Reads only the partition(s) overlapping [start_date, end_date]
      2. Filters by resources and excluded procedures via DuckDB
      3. Returns a small DataFrame (typically 5K-20K rows, not 5M)

    Parameters are tuples (not lists) so Streamlit can hash them for caching.
    `index_hash` MUST be passed as a regular kwarg (no leading underscore)
    so Streamlit's @cache_data includes it in the cache key — when
    partitions change the partition index hash changes and this
    function's cache is correctly invalidated. (A previous version used
    `_index_hash`; Streamlit skips any arg whose name starts with `_`,
    so the cache was only ever busting via TTL or explicit clear, which
    silently served stale data across sessions.)
    `_` is consumed by st.cache_data only when the arg should be skipped
    (e.g. an unhashable DataFrame).

    `date_basis` selects which timestamp column governs both the
    partition-pruning step and the SQL WHERE clause:
      - "complete" (default) -> uses `complete_date` (Date/Time - Complete).
        This is what Analytics + forecast queries use.
      - "drawn" -> uses `drawn_date` (Date/Time - Drawn). Pre-Analytics
        queries pass this so the heatmap groups draws by the day they were
        actually performed, not the day the test completed.

    For drawn-date queries the partition pruner uses the partition's
    complete-date range with a `_DRAWN_LAG_BUFFER` slack on the lower
    side: since `drawn_date <= complete_date` always (you can't
    complete before you draw), a partition with complete_date range
    [c_min, c_max] can contain rows with drawn_date as far back as
    c_min - lag. A 30-day buffer comfortably covers typical lab
    draw -> complete latency (hours to a few days) plus rare slow
    send-out tests, while keeping the typical query at 1-2 partitions.
    """
    index = _load_partition_index()
    if not index:
        return _empty_result_frame()

    if date_basis == "drawn":
        date_col = "drawn_date"
    else:
        date_col = "complete_date"

    needed_keys = []
    for key, meta in index.items():
        c_min = date.fromisoformat(meta["min_date"])
        c_max = date.fromisoformat(meta["max_date"])
        if date_basis == "drawn":
            # Include if some drawn_date in this partition COULD overlap
            # [start_date, end_date]. Drawn is upper-bounded by complete,
            # lower-bounded conservatively by c_min - _DRAWN_LAG_BUFFER.
            if c_min - _DRAWN_LAG_BUFFER <= end_date and c_max >= start_date:
                needed_keys.append(key)
        else:
            if c_min <= end_date and c_max >= start_date:
                needed_keys.append(key)

    if not needed_keys:
        return _empty_result_frame()

    # Stream-filter per partition. Previously this function read every
    # needed partition, pd.concat'd them all into a single combined
    # frame (~500k rows × N partitions), then handed the whole thing
    # to DuckDB to filter down to typically 5-20k rows. Peak memory
    # was ~3× the filtered output (cached frames + combined + result).
    #
    # The streaming pattern below applies the alias/remap passes and
    # the date/resource/procedure filters TO EACH PARTITION
    # individually, then accumulates only the small filtered slices.
    # Peak memory is one partition + the growing list of small slices.
    # DuckDB is no longer needed — the filter logic is simple boolean
    # masking that pandas handles natively, and the input frames
    # already have correct dtypes (object/date from _parquet_bytes_to_df),
    # so the post-DuckDB dtype-fixup pass is also gone.
    #
    # Resource remaps MUST run BEFORE the resource filter — otherwise
    # rows whose stored resource is the old name get dropped by the
    # bench-scoped filter even though the post-remap resource would
    # have matched. Apply aliases too so already-stored partitions
    # render with the short display names. Lazy import to avoid a
    # top-level dependency from storage.py onto parsing.py.
    from parsing import (
        clean_procedure_names as _alias_procs,
        apply_resource_remaps as _remap_resources,
    )

    resource_set = set(resources) if resources else None
    exclude_set  = set(exclude_procs) if exclude_procs else None

    result_frames: list[pd.DataFrame] = []
    for key in sorted(needed_keys):
        df = _read_partition(key)
        if df is None or df.empty:
            continue
        # Copy before mutating — apply_resource_remaps uses
        # df.loc[mask, col] = ... which would otherwise modify the
        # cached partition frame held by _read_partition_cached.
        df = df.copy()
        df = _alias_procs(df)
        df = _remap_resources(df)

        if date_col not in df.columns:
            continue
        mask = (df[date_col] >= start_date) & (df[date_col] <= end_date)
        if resource_set is not None and "Performing Service Resource" in df.columns:
            mask &= df["Performing Service Resource"].isin(resource_set)
        if exclude_set is not None and "Order Procedure" in df.columns:
            mask &= ~df["Order Procedure"].isin(exclude_set)

        sub = df.loc[mask]
        if not sub.empty:
            result_frames.append(sub)

    if not result_frames:
        return _empty_result_frame()
    return pd.concat(result_frames, ignore_index=True)


@st.cache_data(show_spinner=False, ttl=300)
def load_partition_for_month(
    year: int,
    month: int,
    index_hash: str = "",
) -> pd.DataFrame:
    """Load a single month's partition — no filtering applied.

    Used by the sidebar to discover available dates and by forecast training
    to stream one partition at a time.
    """
    key = f"{year:04d}-{month:02d}"
    df = _read_partition(key)
    return df if df is not None else pd.DataFrame()


def get_index_hash() -> str:
    """Return a short, STABLE hash of the partition index for cache-busting.

    Uses SHA-1 (truncated to 16 hex chars) instead of Python's built-in
    `hash()`, which is randomized per process via PYTHONHASHSEED — two
    Streamlit Cloud workers (or two sessions on the same worker after a
    restart) would otherwise produce different cache keys for the same
    on-disk index, defeating shared caching and making state subtly
    process-dependent.
    """
    import hashlib
    index = _load_partition_index()
    payload = json.dumps(index, sort_keys=True, default=str).encode("utf-8")
    return hashlib.sha1(payload).hexdigest()[:16]


def count_rows_in_date_range(start_date: date, end_date: date) -> int:
    """Count rows in a date range using the partition index metadata.

    For exact counts, reads the affected partitions. For a quick estimate
    from the index (e.g. to check if data exists), use get_data_summary().
    """
    index = _load_partition_index()
    if not index:
        return 0

    total = 0
    for key, meta in index.items():
        p_min = date.fromisoformat(meta["min_date"])
        p_max = date.fromisoformat(meta["max_date"])
        if p_min <= end_date and p_max >= start_date:
            # Need exact count - read partition and filter
            df = _read_partition(key)
            if df is not None:
                total += int(
                    ((df["complete_date"] >= start_date) & (df["complete_date"] <= end_date)).sum()
                )
    return total


# ═════════════════════════════════════════════════════════════════════════════
# WRITE OPERATIONS — partition-scoped
# ═════════════════════════════════════════════════════════════════════════════

def ingest_new_data(new_df: pd.DataFrame) -> dict:
    """Ingest a new DataFrame into the partitioned storage.

    Only reads/writes the affected monthly partitions.
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
                (existing_df["Date/Time - Complete"] < new_min) |
                (existing_df["Date/Time - Complete"] > new_max)
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
    _invalidate_index_cache()
    # Re-cache the new index
    st.session_state["_partition_index"] = index

    total_after = sum(p["rows"] for p in index.values())
    return {
        "rows_before": total_before,
        "rows_after": total_after,
        "rows_added": total_after - total_before,
    }


def delete_date_range(start: date, end: date) -> dict:
    """Delete all rows in [start, end] (inclusive) from affected partitions only."""
    index = _load_partition_index()
    if not index:
        return {"rows_removed": 0}

    total_removed = 0
    affected_keys = []
    for key, meta in index.items():
        p_min = date.fromisoformat(meta["min_date"])
        p_max = date.fromisoformat(meta["max_date"])
        if p_min <= end and p_max >= start:
            affected_keys.append(key)

    for key in affected_keys:
        df = _read_partition(key)
        if df is None or df.empty:
            continue

        rows_before = len(df)
        mask = (df["complete_date"] >= start) & (df["complete_date"] <= end)
        df = df[~mask].reset_index(drop=True)
        removed = rows_before - len(df)
        total_removed += removed

        if df.empty:
            sha = index[key]["sha"]
            try:
                _gh_delete_file(_partition_path(key), sha,
                                f"Delete empty partition {key}")
            except Exception:
                pass
            del index[key]
        else:
            pq_bytes = df_to_parquet_bytes(df)
            new_sha = _gh_put_file(
                _partition_path(key), pq_bytes,
                f"Update partition {key} after delete ({removed} rows removed)",
                sha=index[key]["sha"],
            )
            index[key] = {
                "sha": new_sha,
                "rows": len(df),
                "min_date": str(df["complete_date"].min()),
                "max_date": str(df["complete_date"].max()),
                "size_bytes": len(pq_bytes),
            }

    _save_partition_index(index)
    _invalidate_index_cache()
    st.session_state["_partition_index"] = index
    return {"rows_removed": total_removed}


def reset_all_data() -> None:
    """Delete all partitions and the index."""
    index = _load_partition_index()
    for key, meta in index.items():
        try:
            _gh_delete_file(_partition_path(key), meta["sha"],
                            f"Delete partition {key} (full reset)")
        except Exception:
            pass

    _, sha = _gh_get_file(PARTITION_INDEX_PATH)
    if sha:
        try:
            _gh_delete_file(PARTITION_INDEX_PATH, sha,
                            "Delete partition index (full reset)")
        except Exception:
            pass

    _, legacy_sha = _gh_get_file(LEGACY_PARQUET_PATH)
    if legacy_sha:
        try:
            _gh_delete_file(LEGACY_PARQUET_PATH, legacy_sha,
                            "Delete legacy parquet (full reset)")
        except Exception:
            pass

    _invalidate_index_cache()


def ensure_partitioned_storage() -> bool:
    """Ensure data is in partitioned format. Migrates legacy if needed."""
    index = _load_partition_index()
    if index:
        return True
    if _migrate_legacy_parquet():
        _invalidate_index_cache()
        return True
    return False


# ═════════════════════════════════════════════════════════════════════════════
# STREAMING PARTITION READER (for forecast training)
# ═════════════════════════════════════════════════════════════════════════════

def iter_partitions():
    """Yield (key, DataFrame) for each partition, one at a time.

    Used by forecast training to process data partition-by-partition
    without loading the entire dataset into memory at once.
    """
    index = _load_partition_index()
    for key in sorted(index.keys()):
        df = _read_partition(key)
        if df is not None and not df.empty:
            yield key, df
