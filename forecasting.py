"""
forecasting.py — Prophet-based forecast training and caching.

Forecasts are:
  - DECOUPLED from upload — only retrained when the user clicks
    "Refresh Forecast" in the Data Management panel.
  - Stored as pickle files on GitHub (data/forecasts_<MapType>.pkl).
  - Cached in session state for fast access during the same session.
"""

import base64
import pickle
from datetime import date, timedelta

import pandas as pd
import requests
import streamlit as st

from config import (
    EXCLUDE_PROCS,
    FORECAST_HORIZON,
    MAP_TYPES,
    HOUR_LABELS,
)


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
        return None


# ═════════════════════════════════════════════════════════════════════════════
# FORECAST TRAINING
# ═════════════════════════════════════════════════════════════════════════════

def filter_for_map(df: pd.DataFrame, map_type: str, resource_assignments: dict) -> pd.DataFrame:
    """Filter dataset to the resources for one map type."""
    resources = resource_assignments[map_type]
    out = df[df["Performing Service Resource"].isin(resources)].copy()
    out = out[~out["Order Procedure"].isin(EXCLUDE_PROCS)]
    return out


def train_and_cache_forecasts(
    df: pd.DataFrame,
    map_type: str,
    resource_assignments: dict,
) -> None:
    """Train one Prophet model per procedure x hour for the top-30 procedures.

    Payload written to GitHub and stored in session state.
    Sparse combinations (<7 non-zero days) fall back to weekday averages.
    """
    from prophet import Prophet

    filtered = filter_for_map(df, map_type, resource_assignments)
    if filtered.empty:
        return

    last_data_date: date = filtered["complete_date"].max()
    forecast_end:   date = last_data_date + timedelta(days=FORECAST_HORIZON)

    top30 = (
        filtered.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(30).index.tolist()
    )
    filtered = filtered[filtered["Order Procedure"].isin(top30)].copy()

    predictions: dict[tuple[str, int], dict[date, float]] = {}

    for proc in top30:
        proc_df = filtered[filtered["Order Procedure"] == proc]
        for hour in range(24):
            hour_df = proc_df[proc_df["hour"] == hour]
            daily = (
                hour_df.groupby("complete_date")["Complete Volume"]
                .sum().reset_index()
            )
            daily.columns = ["ds", "y"]
            daily["ds"] = pd.to_datetime(daily["ds"])

            if not daily.empty:
                full_range = pd.date_range(daily["ds"].min(), last_data_date)
                daily = (
                    daily.set_index("ds")
                    .reindex(full_range, fill_value=0)
                    .reset_index()
                    .rename(columns={"index": "ds"})
                )

            non_zero = int((daily["y"] > 0).sum()) if not daily.empty else 0

            if non_zero < 7:
                if daily.empty:
                    preds = {
                        last_data_date + timedelta(days=d): 0.0
                        for d in range(1, FORECAST_HORIZON + 1)
                    }
                else:
                    daily["weekday"] = daily["ds"].dt.dayofweek
                    wd_avg = daily.groupby("weekday")["y"].mean().to_dict()
                    preds = {}
                    for d in range(1, FORECAST_HORIZON + 1):
                        fdate = last_data_date + timedelta(days=d)
                        preds[fdate] = max(
                            0.0, round(wd_avg.get(pd.Timestamp(fdate).dayofweek, 0.0), 1)
                        )
                predictions[(proc, hour)] = preds
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
                predictions[(proc, hour)] = preds

    payload = {
        "last_data_date": last_data_date,
        "forecast_end":   forecast_end,
        "predictions":    predictions,
    }

    _write_forecast_to_github(map_type, payload)
    st.session_state[f"forecasts_{map_type}"] = payload


def load_forecasts(map_type: str) -> dict | None:
    """Return the cached forecast payload for map_type, or None."""
    cache_key = f"forecasts_{map_type}"
    if cache_key in st.session_state:
        return st.session_state[cache_key]
    payload = _read_forecast_from_github(map_type)
    if payload is not None:
        st.session_state[cache_key] = payload
    return payload


def retrain_all_forecasts(df: pd.DataFrame, resource_assignments: dict) -> None:
    """Train and cache forecasts for every map type (in-memory df version)."""
    for mt in MAP_TYPES:
        st.session_state.pop(f"forecasts_{mt}", None)
        try:
            train_and_cache_forecasts(df, mt, resource_assignments)
        except Exception as _fc_err:
            st.write(f"Forecast training skipped for {mt}: {_fc_err}")


def retrain_all_forecasts_streaming(resource_assignments: dict) -> None:
    """Train forecasts by streaming partitions — never loads full dataset.

    For each map type:
      1. Stream partitions one at a time
      2. Filter to the map's resources and aggregate daily volumes
         per (procedure, hour) into a dict of {(proc, hour): {date: volume}}
      3. Determine top-30 procedures by total volume
      4. Train Prophet models on the aggregated daily series
    """
    from storage import iter_partitions

    for mt in MAP_TYPES:
        st.session_state.pop(f"forecasts_{mt}", None)
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
    exclude = EXCLUDE_PROCS

    # Phase 1: Stream partitions and aggregate daily volumes for BOTH bases.
    # daily_agg_complete[(proc, hour)][date] = volume
    # daily_agg_inlab   [(proc, hour)][date] = volume
    daily_agg_complete: dict[tuple[str, int], dict] = {}
    daily_agg_inlab: dict[tuple[str, int], dict] = {}
    proc_totals: dict[str, float] = {}
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
            for _, row in grouped_c.iterrows():
                proc = row["Order Procedure"]
                hour = int(row["hour"])
                d = row["complete_date"]
                vol = float(row["Complete Volume"])
                key = (proc, hour)
                daily_agg_complete.setdefault(key, {})
                daily_agg_complete[key][d] = daily_agg_complete[key].get(d, 0.0) + vol
                proc_totals[proc] = proc_totals.get(proc, 0.0) + vol
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
                for _, row in grouped_i.iterrows():
                    proc = row["Order Procedure"]
                    hour = int(row["_ih"])
                    d = row["inlab_date"]
                    vol = float(row["Complete Volume"])
                    key = (proc, hour)
                    daily_agg_inlab.setdefault(key, {})
                    daily_agg_inlab[key][d] = daily_agg_inlab[key].get(d, 0.0) + vol
                del grouped_i

        del filt

    if last_data_date_complete is None and last_data_date_inlab is None:
        return

    # Top-30 procedures ranked by Completed totals (same ranking for both bases)
    top30 = sorted(proc_totals.keys(), key=lambda p: proc_totals[p], reverse=True)[:30]

    predictions_complete = _train_models_for_basis(
        top30, daily_agg_complete, last_data_date_complete
    ) if last_data_date_complete is not None else {}

    predictions_inlab = _train_models_for_basis(
        top30, daily_agg_inlab, last_data_date_inlab
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
    st.session_state[f"forecasts_{map_type}"] = payload


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

def build_forecast_pivot(
    fc_data: dict,
    selected_date: date,
    hour_range: tuple[int, int],
    time_basis: str = "Completed",
) -> tuple[pd.DataFrame | None, list[int]]:
    """Build a procedure x hour pivot from forecast predictions for a future date.

    time_basis selects which prediction set to use:
      - "Completed" → predictions_complete (based on Date/Time - Complete)
      - "In-Lab"    → predictions_inlab    (based on Date/Time - In Lab)
    Legacy payloads without split predictions fall back to "predictions".
    """
    h_start, h_end = hour_range
    hours = list(range(h_start, h_end + 1))
    if time_basis == "In-Lab":
        preds = fc_data.get("predictions_inlab") or fc_data.get("predictions", {})
    else:
        preds = fc_data.get("predictions_complete") or fc_data.get("predictions", {})

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
    pivot = pivot.sort_values("Total", ascending=False).head(30)
    pivot.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in pivot.columns]
    return pivot, hours
