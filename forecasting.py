"""
forecasting.py — Prophet-based forecast training and caching.

Forecasts are:
  - DECOUPLED from upload — only retrained when the user clicks
    "Refresh Forecast" in the Data Management panel.
  - Stored as pickle files on GitHub (data/forecasts_<MapType>.pkl).
  - Cached process-wide via @st.cache_data on load_forecasts() (TTL 10 min)
    so payloads don't accumulate per-session in session_state.
"""

import base64
import logging
import pickle
from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

from config import (
    FORECAST_HORIZON,
    MAP_TYPES,
    HOUR_LABELS,
)
from analytics.filters import EXCLUDED_PROCEDURES

logger = logging.getLogger(__name__)


# ═════════════════════════════════════════════════════════════════════════════
# GITHUB I/O FOR FORECAST PICKLES
# ═════════════════════════════════════════════════════════════════════════════

def _gh_headers() -> dict:
    return {
        "Authorization": f"Bearer {st.secrets['github']['token']}",
        "Accept": "application/vnd.github+json",
    }


def _gh_repo() -> tuple[str, str]:
    repo = st.secrets["github"]["repo"]
    owner, repo_name = repo.split("/", 1)
    return owner, repo_name


def _forecast_gh_path(map_type: str) -> str:
    return f"data/forecasts_{map_type.replace(' ', '_')}.pkl"


def _write_forecast_to_github(map_type: str, payload: dict) -> None:
    if "github" not in st.secrets:
        return
    owner, repo = _gh_repo()
    gh_path = _forecast_gh_path(map_type)
    url     = f"https://api.github.com/repos/{owner}/{repo}/contents/{gh_path}"
    headers = _gh_headers()

    pkl_bytes = pickle.dumps(payload)
    b64       = base64.b64encode(pkl_bytes).decode()

    resp = requests.get(url, headers=headers, timeout=15)
    sha  = resp.json().get("sha") if resp.status_code == 200 else None

    body: dict = {"message": f"Update forecasts for {map_type}", "content": b64}
    if sha:
        body["sha"] = sha
    requests.put(url, headers=headers, json=body, timeout=30)


def _read_forecast_from_github(map_type: str) -> dict | None:
    if "github" not in st.secrets:
        return None
    owner, repo = _gh_repo()
    url     = f"https://api.github.com/repos/{owner}/{repo}/contents/{_forecast_gh_path(map_type)}"
    headers = _gh_headers()
    try:
        resp = requests.get(url, headers=headers, timeout=15)
        if resp.status_code != 200:
            return None
        return pickle.loads(base64.b64decode(resp.json()["content"]))
    except Exception:
        logger.exception(
            "Failed to read/unpickle forecast for %s — UI will show 'No forecast available'",
            map_type,
        )
        return None


# ═════════════════════════════════════════════════════════════════════════════
# FORECAST TRAINING
# ═════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False, ttl=600)
def load_forecasts(map_type: str) -> dict | None:
    """Return the cached forecast payload for map_type, or None.

    Backed by @st.cache_data (TTL 10 min) instead of raw session_state.
    A forecast payload can hold ~21k (proc, hour) → {date: float}
    entries; storing one per map_type per user-session in session_state
    accumulated without bound and was a real OOM risk on Streamlit
    Cloud's 1 GB cap. The TTL-backed cache_data approach is shared
    across sessions on the same worker (fewer GitHub API hits) and
    bounded by Streamlit's cache-eviction policy.
    """
    return _read_forecast_from_github(map_type)


def retrain_all_forecasts_streaming(resource_assignments: dict) -> None:
    """Train forecasts by streaming partitions — never loads full dataset.

    For each map type:
      1. Stream partitions one at a time
      2. Filter to the map's resources and aggregate daily volumes
         per (procedure, hour) into a dict of {(proc, hour): {date: volume}}
      3. Determine top-30 procedures by total volume
      4. Train Prophet models on the aggregated daily series
    """
    # Bust the read cache up front so any reads during training don't
    # return stale data; the per-map _train_forecasts_streaming call
    # also clears after each successful write.
    load_forecasts.clear()
    for mt in MAP_TYPES:
        try:
            _train_forecasts_streaming(mt, resource_assignments)
        except Exception as _fc_err:
            st.write(f"Forecast training skipped for {mt}: {_fc_err}")


def _train_forecasts_streaming(map_type: str, resource_assignments: dict) -> None:
    """Stream partitions and train forecasts without loading all data.

    Produces TWO prediction sets per map type:
      - predictions_complete: indexed by (complete_date, hour)
      - predictions_inlab:    indexed by (inlab_date,   inlab_hour)

    The basis is chosen at query time via build_forecast_pivot(..., time_basis=...).
    """
    from storage import iter_partitions

    resources = set(resource_assignments[map_type])
    exclude = EXCLUDED_PROCEDURES

    # Phase 1: Stream partitions and aggregate daily volumes for BOTH bases.
    # daily_agg_complete[(proc, hour)][date] = volume
    # daily_agg_inlab   [(proc, hour)][date] = volume
    daily_agg_complete: dict[tuple[str, int], dict] = {}
    daily_agg_inlab: dict[tuple[str, int], dict] = {}
    # Per-basis totals. We rank top-30 separately for each basis (a
    # procedure can be high-volume by Complete count but low by In-Lab
    # count or vice versa) and then train on the UNION so neither view
    # silently drops a procedure that's a top performer for its basis.
    proc_totals_complete: dict[str, float] = {}
    proc_totals_inlab: dict[str, float] = {}
    last_data_date_complete = None
    last_data_date_inlab = None

    for _key, partition_df in iter_partitions():
        filt = partition_df[
            partition_df["Performing Service Resource"].isin(resources) &
            ~partition_df["Order Procedure"].isin(exclude)
        ]
        if filt.empty:
            continue

        # ── Completed basis ─────────────────────────────────────────────
        c = filt.dropna(subset=["complete_date", "hour"])
        if not c.empty:
            part_max_c = c["complete_date"].max()
            if last_data_date_complete is None or part_max_c > last_data_date_complete:
                last_data_date_complete = part_max_c

            grouped_c = (
                c.groupby(["Order Procedure", "hour", "complete_date"])["Complete Volume"]
                .sum().reset_index()
            )
            # itertuples is ~50x faster than iterrows on a multi-thousand-row
            # groupby frame; semantics unchanged.
            for proc, hour, d, vol in grouped_c.itertuples(index=False, name=None):
                hour = int(hour)
                vol = float(vol)
                key = (proc, hour)
                daily_agg_complete.setdefault(key, {})
                daily_agg_complete[key][d] = daily_agg_complete[key].get(d, 0.0) + vol
                proc_totals_complete[proc] = proc_totals_complete.get(proc, 0.0) + vol
            del grouped_c

        # ── In-Lab basis ────────────────────────────────────────────────
        if "inlab_date" in filt.columns and "inlab_hour" in filt.columns:
            il = filt.dropna(subset=["inlab_date", "inlab_hour"])
            if not il.empty:
                il = il.copy()
                il["_ih"] = il["inlab_hour"].astype(int)
                part_max_i = il["inlab_date"].max()
                if last_data_date_inlab is None or part_max_i > last_data_date_inlab:
                    last_data_date_inlab = part_max_i

                grouped_i = (
                    il.groupby(["Order Procedure", "_ih", "inlab_date"])["Complete Volume"]
                    .sum().reset_index()
                )
                for proc, hour, d, vol in grouped_i.itertuples(index=False, name=None):
                    hour = int(hour)
                    vol = float(vol)
                    key = (proc, hour)
                    daily_agg_inlab.setdefault(key, {})
                    daily_agg_inlab[key][d] = daily_agg_inlab[key].get(d, 0.0) + vol
                    proc_totals_inlab[proc] = proc_totals_inlab.get(proc, 0.0) + vol
                del grouped_i

        del filt

    if last_data_date_complete is None and last_data_date_inlab is None:
        return

    # Top-30 by EACH basis, unioned. Without this, a procedure that's
    # high-volume by In-Lab but low by Complete would be dropped from
    # the In-Lab forecast view entirely.
    top30_complete = sorted(
        proc_totals_complete.keys(),
        key=lambda p: proc_totals_complete[p],
        reverse=True,
    )[:30]
    top30_inlab = sorted(
        proc_totals_inlab.keys(),
        key=lambda p: proc_totals_inlab[p],
        reverse=True,
    )[:30]
    top_procs = sorted(set(top30_complete) | set(top30_inlab))

    predictions_complete = _train_models_for_basis(
        top_procs, daily_agg_complete, last_data_date_complete
    ) if last_data_date_complete is not None else {}

    predictions_inlab = _train_models_for_basis(
        top_procs, daily_agg_inlab, last_data_date_inlab
    ) if last_data_date_inlab is not None else {}

    # Use the later of the two as the "canonical" last/forecast-end date.
    last_data_date = max(
        d for d in (last_data_date_complete, last_data_date_inlab) if d is not None
    )
    forecast_end = last_data_date + timedelta(days=FORECAST_HORIZON)

    payload = {
        "last_data_date": last_data_date,
        "last_data_date_complete": last_data_date_complete,
        "last_data_date_inlab": last_data_date_inlab,
        "forecast_end": forecast_end,
        "predictions_complete": predictions_complete,
        "predictions_inlab": predictions_inlab,
        # Back-compat: older code paths reading "predictions" get Completed.
        "predictions": predictions_complete,
    }

    _write_forecast_to_github(map_type, payload)
    # Bust the read cache so the next load_forecasts() call returns
    # the just-written payload instead of the stale cached version.
    load_forecasts.clear()


def _train_models_for_basis(
    top30: list[str],
    daily_agg: dict[tuple[str, int], dict],
    last_data_date: date,
) -> dict[tuple[str, int], dict]:
    """Train Prophet (or weekday-avg fallback) models for one time basis."""
    from prophet import Prophet

    predictions: dict[tuple[str, int], dict] = {}

    for proc in top30:
        for hour in range(24):
            key = (proc, hour)
            daily_dict = daily_agg.get(key, {})

            if not daily_dict:
                predictions[key] = {
                    last_data_date + timedelta(days=d): 0.0
                    for d in range(1, FORECAST_HORIZON + 1)
                }
                continue

            daily = pd.DataFrame([
                {"ds": pd.Timestamp(d), "y": v}
                for d, v in daily_dict.items()
            ]).sort_values("ds")

            full_range = pd.date_range(daily["ds"].min(), last_data_date)
            daily = (
                daily.set_index("ds")
                .reindex(full_range, fill_value=0)
                .reset_index()
                .rename(columns={"index": "ds"})
            )

            non_zero = int((daily["y"] > 0).sum())

            if non_zero < 7:
                daily["weekday"] = daily["ds"].dt.dayofweek
                wd_avg = daily.groupby("weekday")["y"].mean().to_dict()
                preds = {}
                for d in range(1, FORECAST_HORIZON + 1):
                    fdate = last_data_date + timedelta(days=d)
                    preds[fdate] = max(
                        0.0, round(wd_avg.get(pd.Timestamp(fdate).dayofweek, 0.0), 1)
                    )
                predictions[key] = preds
            else:
                m = Prophet(
                    weekly_seasonality=True,
                    daily_seasonality=False,
                    yearly_seasonality=False,
                    changepoint_prior_scale=0.05,
                )
                m.fit(daily)
                future = m.make_future_dataframe(periods=FORECAST_HORIZON)
                fc = m.predict(future)
                fc = fc[fc["ds"] > pd.Timestamp(last_data_date)]
                preds = {
                    row["ds"].date(): max(0.0, round(row["yhat"], 1))
                    for _, row in fc.iterrows()
                }
                predictions[key] = preds

    return predictions


# ═════════════════════════════════════════════════════════════════════════════
# FORECAST PIVOT BUILDER
# ═════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner=False, ttl=300)
def build_forecast_pivot(
    _fc_data: dict,
    selected_date: date,
    hour_range: tuple[int, int],
    time_basis: str = "Completed",
    top_n: int | None = 30,
    cache_key: str = "",
) -> tuple[pd.DataFrame | None, list[int]]:
    """Build a procedure x hour pivot from forecast predictions for a future date.

    time_basis selects which prediction set to use:
      - "Completed" -> predictions_complete (based on Date/Time - Complete)
      - "In-Lab"    -> predictions_inlab    (based on Date/Time - In Lab)
    Legacy payloads without split predictions fall back to "predictions".

    `top_n` caps the returned procedure rows:
      - int  -> keep the top-N forecasted procedures by full-day sum
      - None -> keep every forecasted procedure (the sidebar's "All" option).
    Forecasts are only trained for the top 30 procedures by historical
    volume (train_forecasts uses head(30)), so the effective ceiling
    is 30 even when top_n is None or > 30.

    `_fc_data` is underscore-prefixed so Streamlit skips hashing the dict
    (would be expensive for a 720+ key forecast payload). `cache_key`
    carries the (map_type, last_data_date) fingerprint that callers must
    supply so the cache busts when forecasts are retrained.
    """
    _ = cache_key  # consumed by st.cache_data for cache-key fingerprinting
    h_start, h_end = hour_range
    hours = list(range(h_start, h_end + 1))
    if time_basis == "In-Lab":
        preds = _fc_data.get("predictions_inlab") or _fc_data.get("predictions", {})
    else:
        preds = _fc_data.get("predictions_complete") or _fc_data.get("predictions", {})

    rows: dict = {}
    for (proc, hour), date_map in preds.items():
        if hour not in hours:
            continue
        val = date_map.get(selected_date, 0.0)
        if val:
            rows.setdefault(proc, {})[hour] = val

    if not rows:
        return None, hours

    pivot = pd.DataFrame(rows).T.reindex(columns=hours, fill_value=0.0)
    pivot = pivot.fillna(0)
    pivot["Total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("Total", ascending=False)
    if top_n is not None:
        pivot = pivot.head(top_n)
    pivot.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in pivot.columns]
    return pivot, hours
