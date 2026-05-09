import calendar as _cal
from datetime import date

import pandas as pd
import streamlit as st

from config import HOUR_LABELS
from storage import load_filtered_data, get_index_hash, storage_is_configured
from forecasting import load_forecasts, build_forecast_pivot


def build_pivot(
    df: pd.DataFrame,
    selected_date: date,
    hour_range: tuple,
) -> tuple:
    h_start, h_end = hour_range
    hours   = list(range(h_start, h_end + 1))
    df_date = df[df["complete_date"] == selected_date].copy()
    df_dh   = df_date[df_date["hour"].isin(hours)].copy()

    if df_dh.empty:
        return None, None, df_date, hours

    top30 = (
        df_date.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(30).index.tolist()
    )
    df_dh = df_dh[df_dh["Order Procedure"].isin(top30)].copy()
    if df_dh.empty:
        return None, None, df_date, hours

    pivot = (
        df_dh.pivot_table(
            index="Order Procedure", columns="hour",
            values="Complete Volume", aggfunc="sum", fill_value=0.0,
        ).reindex(columns=hours, fill_value=0.0)
    )
    pivot["Total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("Total", ascending=False)
    pivot.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in pivot.columns]
    return pivot, df_dh, df_date, hours


def build_monthly_pivot(
    df: pd.DataFrame, year: int, month: int,
) -> tuple:
    month_start = date(year, month, 1)
    month_end   = date(year, month, _cal.monthrange(year, month)[1])
    month_df    = df[
        (df["complete_date"] >= month_start) &
        (df["complete_date"] <= month_end)
    ].copy()

    if month_df.empty:
        return None, 0, month_df

    top30 = (
        month_df.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(30).index.tolist()
    )
    month_df = month_df[month_df["Order Procedure"].isin(top30)].copy()

    pivot = (
        month_df.pivot_table(
            index="Order Procedure", columns="hour",
            values="Complete Volume", aggfunc="sum", fill_value=0,
        ).reindex(columns=list(range(24)), fill_value=0)
    )

    n_days = int(month_df["complete_date"].nunique())
    avg = pivot / n_days
    avg["Total"] = avg.sum(axis=1)
    avg = avg.sort_values("Total", ascending=False)
    avg.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in avg.columns]
    return avg, n_days, month_df


def build_weekday_pivot(
    month_df: pd.DataFrame, year: int, month: int,
) -> tuple:
    if month_df.empty:
        return None, {}

    df = month_df.copy()
    df["weekday"] = pd.to_datetime(df["complete_date"]).dt.dayofweek

    pivot = (
        df.pivot_table(
            index="weekday", columns="hour",
            values="Complete Volume", aggfunc="sum", fill_value=0,
        ).reindex(index=list(range(7)), columns=list(range(24)), fill_value=0)
    )

    month_start = date(year, month, 1)
    month_end   = date(year, month, _cal.monthrange(year, month)[1])
    wd_counts: dict = {wd: 0 for wd in range(7)}
    for d in pd.date_range(month_start, month_end):
        wd_counts[d.dayofweek] += 1

    for wd in range(7):
        pivot.loc[wd] = pivot.loc[wd] / max(wd_counts[wd], 1)

    pivot["Total"] = pivot[list(range(24))].sum(axis=1)

    _day_names = ["Monday", "Tuesday", "Wednesday", "Thursday",
                  "Friday", "Saturday", "Sunday"]
    pivot.index = [f"{_day_names[wd]}  (×{wd_counts[wd]})" for wd in range(7)]
    pivot.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in pivot.columns]
    return pivot, wd_counts
