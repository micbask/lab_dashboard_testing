"""
storage.py — Partitioned Parquet storage on GitHub with DuckDB query layer.

Data architecture for 5M+ rows:
  - Data is partitioned by month: data/partitions/YYYY-MM.parquet
  - A lightweight JSON index (data/partition_index.json) tracks each
    partition's date range, row count, and GitHub blob SHA.
  - Uploads only read/write the affected monthly partitions.
  - DuckDB serves as an in-process SQL query layer over partitioned
    Parquet files, enabling filtered reads without loading everything.
  - Legacy single-file (data/lab_data.parquet) is auto-migrated on
    first access.

GitHub API constraints:
  - Contents API has ~100 MB file size limit.
  - Monthly partitions stay well under this limit even at 5M+ total rows.
"""

import base64
import io
import json
import time
from datetime import date
from typing import Optional

import duckdb
import pandas as pd
import requests
import streamlit as st

from config import (
    PARTITION_DIR,
    PARTITION_INDEX_PATH,
    LEGACY_PARQUET_PATH,
)


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


def _gh_list_dir(path: str) -> list[dict]:
    """List files in a GitHub directory. Returns list of {name, path, sha, size}."""
    owner, repo = _gh_repo()
    resp = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
        headers=_gh_headers(), timeout=15,
    )
    if resp.status_code == 404:
        return []
    resp.raise_for_status()
    items = resp.json()
    if isinstance(items, list):
        return [{"name": i["name"], "path": i["path"], "sha": i["sha"], "size": i.get("size", 0)} for i in items]
    return []


# ═════════════════════════════════════════════════════════════════════════════
# PARTITION INDEX
# ═════════════════════════════════════════════════════════════════════════════

def _load_partition_index() -> dict:
    """Load the partition index from GitHub. Returns {} if not found."""
    raw, _ = _gh_get_file(PARTITION_INDEX_PATH)
    if raw is None:
        return {}
    try:
        return json.loads(raw.decode("utf-8"))
    except Exception:
        return {}


def _save_partition_index(index: dict) -> None:
    """Save the partition index to GitHub."""
    content = json.dumps(index, indent=2, default=str).encode("utf-8")
    _, sha = _gh_get_file(PARTITION_INDEX_PATH)
    _gh_put_file(
        PARTITION_INDEX_PATH, content,
        "Update partition index",
        sha=sha,
    )


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
    return df


# ═════════════════════════════════════════════════════════════════════════════
# LEGACY MIGRATION
# ═════════════════════════════════════════════════════════════════════════════

def _migrate_legacy_parquet() -> bool:
    """Migrate the legacy single-file parquet to partitioned storage.

    Returns True if migration was performed, False if no legacy file found.
    """
    raw, legacy_sha = _gh_get_file(LEGACY_PARQUET_PATH)
    if raw is None or len(raw) < 100:
        return False

    df = _parquet_bytes_to_df(raw)
    if df.empty:
        return False

    # Write partitions
    _write_partitions_from_df(df)

    # Delete legacy file
    if legacy_sha:
        try:
            _gh_delete_file(LEGACY_PARQUET_PATH, legacy_sha, "Remove legacy monolithic parquet (migrated to partitions)")
        except Exception:
            pass  # Non-critical: legacy file can coexist

    return True


# ═════════════════════════════════════════════════════════════════════════════
# PARTITION READ/WRITE
# ═════════════════════════════════════════════════════════════════════════════

def _write_partitions_from_df(df: pd.DataFrame) -> dict:
    """Split a DataFrame by month and write each partition to GitHub.

    Returns the updated partition index.
    """
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

        # Get existing SHA if partition already exists
        existing_sha = index.get(key, {}).get("sha")
        path = _partition_path(key)

        new_sha = _gh_put_file(
            path, pq_bytes,
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

    # Clean up temp column from original df
    if "_partition_key" in df.columns:
        df.drop(columns=["_partition_key"], inplace=True)

    _save_partition_index(index)
    return index


def _read_partition(key: str) -> Optional[pd.DataFrame]:
    """Read a single partition from GitHub."""
    raw, _ = _gh_get_file(_partition_path(key))
    if raw is None:
        return None
    return _parquet_bytes_to_df(raw)


# ═════════════════════════════════════════════════════════════════════════════
# PUBLIC STORAGE API
# ═════════════════════════════════════════════════════════════════════════════

def storage_is_configured() -> bool:
    """Check if GitHub storage is configured."""
    return "github" in st.secrets


def get_data_summary() -> dict:
    """Return summary of stored data: total_rows, min_date, max_date, partitions."""
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


@st.cache_data(show_spinner="Loading data…", ttl=300)
def load_data_for_date_range(
    start_date: date,
    end_date: date,
    _cache_key: str = "",
) -> pd.DataFrame:
    """Load only the partitions covering the requested date range.

    Uses DuckDB to filter rows efficiently. _cache_key is for Streamlit
    cache busting (pass partition index hash or similar).
    """
    # Determine which partitions to load
    index = _load_partition_index()
    if not index:
        return pd.DataFrame()

    needed_keys = set()
    for key, meta in index.items():
        p_min = date.fromisoformat(meta["min_date"])
        p_max = date.fromisoformat(meta["max_date"])
        if p_min <= end_date and p_max >= start_date:
            needed_keys.add(key)

    if not needed_keys:
        return pd.DataFrame()

    # Download needed partitions and combine
    frames = []
    for key in sorted(needed_keys):
        df = _read_partition(key)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    combined = pd.concat(frames, ignore_index=True)

    # Use DuckDB to filter to exact date range
    con = duckdb.connect()
    con.register("data", combined)
    result = con.execute(
        "SELECT * FROM data WHERE complete_date >= ? AND complete_date <= ?",
        [start_date, end_date],
    ).fetchdf()
    con.close()

    return result


def load_all_data(_cache_key: str = "") -> pd.DataFrame:
    """Load all partitions into a single DataFrame.

    For operations that truly need the full dataset (e.g. forecast training).
    Uses caching to avoid repeated downloads within the same session.
    """
    index = _load_partition_index()
    if not index:
        return pd.DataFrame()

    frames = []
    for key in sorted(index.keys()):
        df = _read_partition(key)
        if df is not None and not df.empty:
            frames.append(df)

    if not frames:
        return pd.DataFrame()

    return pd.concat(frames, ignore_index=True)


def get_partition_index_hash() -> str:
    """Return a hash of the partition index for cache-busting."""
    index = _load_partition_index()
    return str(hash(json.dumps(index, sort_keys=True, default=str)))


def ingest_new_data(new_df: pd.DataFrame) -> dict:
    """Ingest a new DataFrame into the partitioned storage.

    For each month in the new data:
      1. Read the existing partition (if any)
      2. Merge using time-window deduplication
      3. Write the updated partition back

    Returns stats dict.
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

        # Read existing partition
        existing_df = _read_partition(key)

        if existing_df is not None and not existing_df.empty:
            # Merge: time-window deduplication
            # New data takes priority in overlap window
            new_min = new_group["Date/Time - Complete"].min()
            new_max = new_group["Date/Time - Complete"].max()
            # Remove rows from existing that fall in the new data's time window
            existing_trimmed = existing_df[
                (existing_df["Date/Time - Complete"] < new_min) |
                (existing_df["Date/Time - Complete"] > new_max)
            ]
            merged = pd.concat([existing_trimmed, new_group], ignore_index=True)
            merged = merged.sort_values("Date/Time - Complete").reset_index(drop=True)
        else:
            merged = new_group.reset_index(drop=True)

        # Write partition
        pq_bytes = df_to_parquet_bytes(merged)
        existing_sha = index.get(key, {}).get("sha")
        path = _partition_path(key)

        new_sha = _gh_put_file(
            path, pq_bytes,
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

    # Clean up temp column
    if "_partition_key" in new_df.columns:
        new_df.drop(columns=["_partition_key"], inplace=True)

    _save_partition_index(index)
    total_after = sum(p["rows"] for p in index.values())

    return {
        "rows_before": total_before,
        "rows_after": total_after,
        "rows_added": total_after - total_before,
    }


def delete_date_range(start: date, end: date) -> dict:
    """Delete all rows in [start, end] (inclusive) from affected partitions.

    Returns stats dict with rows removed.
    """
    index = _load_partition_index()
    if not index:
        return {"rows_removed": 0}

    total_removed = 0

    # Find affected partitions
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
            # Delete empty partition
            sha = index[key]["sha"]
            try:
                _gh_delete_file(
                    _partition_path(key), sha,
                    f"Delete empty partition {key}",
                )
            except Exception:
                pass
            del index[key]
        else:
            # Rewrite partition
            pq_bytes = df_to_parquet_bytes(df)
            new_sha = _gh_put_file(
                _partition_path(key), pq_bytes,
                f"Update partition {key} after date range delete ({removed} rows removed)",
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
    return {"rows_removed": total_removed}


def reset_all_data() -> None:
    """Delete all partitions and the index."""
    index = _load_partition_index()
    for key, meta in index.items():
        try:
            _gh_delete_file(
                _partition_path(key), meta["sha"],
                f"Delete partition {key} (full reset)",
            )
        except Exception:
            pass

    # Delete index
    _, sha = _gh_get_file(PARTITION_INDEX_PATH)
    if sha:
        try:
            _gh_delete_file(PARTITION_INDEX_PATH, sha, "Delete partition index (full reset)")
        except Exception:
            pass

    # Also delete legacy file if present
    _, legacy_sha = _gh_get_file(LEGACY_PARQUET_PATH)
    if legacy_sha:
        try:
            _gh_delete_file(LEGACY_PARQUET_PATH, legacy_sha, "Delete legacy parquet (full reset)")
        except Exception:
            pass


def ensure_partitioned_storage() -> bool:
    """Ensure data is in partitioned format. Migrates legacy if needed.

    Returns True if data exists (in any format), False if no data at all.
    """
    index = _load_partition_index()
    if index:
        return True

    # Check for legacy file and migrate
    if _migrate_legacy_parquet():
        return True

    return False


def query_data(sql: str, params: list = None) -> pd.DataFrame:
    """Run a DuckDB SQL query across all partitions.

    Loads needed partitions into DuckDB and runs the query.
    The 'data' table is available in the query.
    """
    all_df = load_all_data()
    if all_df.empty:
        return pd.DataFrame()

    con = duckdb.connect()
    con.register("data", all_df)
    if params:
        result = con.execute(sql, params).fetchdf()
    else:
        result = con.execute(sql).fetchdf()
    con.close()
    return result
