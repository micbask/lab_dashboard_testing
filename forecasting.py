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
    """Train and cache forecasts for every map type."""
    for mt in MAP_TYPES:
        st.session_state.pop(f"forecasts_{mt}", None)
        try:
            train_and_cache_forecasts(df, mt, resource_assignments)
        except Exception as _fc_err:
            st.write(f"Forecast training skipped for {mt}: {_fc_err}")


# ═════════════════════════════════════════════════════════════════════════════
# FORECAST PIVOT BUILDER
# ═════════════════════════════════════════════════════════════════════════════

def build_forecast_pivot(
    fc_data: dict,
    selected_date: date,
    hour_range: tuple[int, int],
) -> tuple[pd.DataFrame | None, list[int]]:
    """Build a procedure x hour pivot from forecast predictions for a future date."""
    h_start, h_end = hour_range
    hours = list(range(h_start, h_end + 1))
    preds = fc_data.get("predictions", {})

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
