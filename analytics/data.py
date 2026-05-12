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
    top_n: int = 30,
) -> tuple:
    h_start, h_end = hour_range
    hours   = list(range(h_start, h_end + 1))
    df_date = df[df["complete_date"] == selected_date].copy()
    df_dh   = df_date[df_date["hour"].isin(hours)].copy()

    if df_dh.empty:
        return None, None, df_date, hours

    top_procs = (
        df_date.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(top_n).index.tolist()
    )
    df_dh = df_dh[df_dh["Order Procedure"].isin(top_procs)].copy()
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
    top_n: int = 30,
) -> tuple:
    month_start = date(year, month, 1)
    month_end   = date(year, month, _cal.monthrange(year, month)[1])
    month_df    = df[
        (df["complete_date"] >= month_start) &
        (df["complete_date"] <= month_end)
    ].copy()

    if month_df.empty:
        return None, 0, month_df

    top_procs = (
        month_df.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(top_n).index.tolist()
    )
    month_df = month_df[month_df["Order Procedure"].isin(top_procs)].copy()

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


def load_monthly_avg_for_comparison(
    df: pd.DataFrame,
    selected_date: date,
    procedures: list,
) -> pd.DataFrame:
    """Build a procedure × hour-of-day average-per-day pivot for the month
    that contains `selected_date`, restricted to the given procedures.

    Used by the Daily heatmap's hover tooltip in analytics/dashboard.py to
    compare today's count to the month's per-day average for the same
    (procedure, hour) cell. The returned DataFrame is indexed by procedure
    (subset of `procedures`) with integer hour columns 0..23. Each cell is
    the month-summed count for that procedure/hour divided by the number
    of distinct dates that have any data in the month.

    Returns an empty DataFrame if `df` has no rows in the selected month or
    no rows for any of the requested procedures.
    """
    if df is None or df.empty or not procedures:
        return pd.DataFrame()

    year, month = selected_date.year, selected_date.month
    month_start = date(year, month, 1)
    month_end   = date(year, month, _cal.monthrange(year, month)[1])

    month_df = df[
        (df["complete_date"] >= month_start) &
        (df["complete_date"] <= month_end) &
        (df["Order Procedure"].isin(procedures))
    ]
    if month_df.empty:
        return pd.DataFrame()

    pivot = (
        month_df.pivot_table(
            index="Order Procedure", columns="hour",
            values="Complete Volume", aggfunc="sum", fill_value=0,
        ).reindex(index=procedures, columns=list(range(24)), fill_value=0)
    )
    n_days = max(int(month_df["complete_date"].nunique()), 1)
    return pivot / n_days


# ════════════════════════════════════════════════════════════════════════════
# TAT DATA LAYER (Phase 1)
# ════════════════════════════════════════════════════════════════════════════

# Accession-number prefixes that identify each facility. The TAT view
# filters on "Accession Nbr - Formatted" (more accurate than the older
# Patient-Location prefix because every sample carries an accession
# number, including outpatient cases where Patient Location can be
# blank or non-USC/non-NCI). Keck accessions start with "267"; Norris
# accessions start with "330".
_TAT_FACILITY_PREFIX = {
    "Keck":   "267",
    "Norris": "330",
}

# Stat columns reported per priority group inside `build_tat_table`'s
# MultiIndex output. The order here is the order they appear in the
# rendered table.
_TAT_STAT_COLS = ["n", "Mean", "% <1h"]


@st.cache_data(show_spinner=False, ttl=300)
def load_tat_data(
    date_str: str, view: str, facility: str,
) -> pd.DataFrame:
    """Load Turn-Around-Time data for a facility/date range from the
    partitioned Parquet store.

    Parameters
    ----------
    date_str : str
        'YYYY-MM-DD' for `view='Daily'`; 'YYYY-MM' for `view='Monthly'`.
    view : str
        'Daily' or 'Monthly'.
    facility : str
        'Keck'   (Accession Nbr - Formatted starts with '267') or
        'Norris' (Accession Nbr - Formatted starts with '330').

    Returns a DataFrame with columns
        ['Order Procedure', 'Collection Priority', 'TAT_minutes']
    where TAT_minutes is a float computed as
    (Date/Time - Complete) − (Date/Time - Drawn) in minutes. Rows whose
    Date/Time - Drawn is null are dropped before the computation.

    Uses the same partition-aware loader (`load_filtered_data`) as the
    existing volume heatmaps; the resource and procedure-exclusion filters
    are bypassed by passing empty tuples, then the facility filter is
    applied on `Accession Nbr - Formatted` (267 = Keck, 330 = Norris)
    after the partition load.
    """
    if view == "Daily":
        target_date = date.fromisoformat(date_str)
        start_d, end_d = target_date, target_date
    else:  # Monthly — date_str is 'YYYY-MM'
        year  = int(date_str[:4])
        month = int(date_str[5:7])
        start_d = date(year, month, 1)
        end_d   = date(year, month, _cal.monthrange(year, month)[1])

    _empty = pd.DataFrame(
        columns=["Order Procedure", "Collection Priority", "TAT_minutes"]
    )

    if not storage_is_configured():
        return _empty

    df = load_filtered_data(
        start_date=start_d,
        end_date=end_d,
        resources=(),       # skip resource filter — TAT spans all benches
        exclude_procs=(),   # skip exclusion filter — TAT spans all procs
        _index_hash=get_index_hash(),
    )
    if df.empty:
        return _empty

    needed = [
        "Order Procedure", "Collection Priority", "Accession Nbr - Formatted",
        "Date/Time - Complete", "Date/Time - Drawn",
    ]
    keep = [c for c in needed if c in df.columns]
    if "Date/Time - Complete" not in keep or "Date/Time - Drawn" not in keep:
        return _empty
    df = df[keep].copy()

    df["Date/Time - Complete"] = pd.to_datetime(
        df["Date/Time - Complete"], errors="coerce"
    )
    df["Date/Time - Drawn"] = pd.to_datetime(
        df["Date/Time - Drawn"], errors="coerce"
    )

    # Re-apply the date-range filter on the actual Complete timestamp (the
    # partition loader filtered on the derived `complete_date` which is
    # day-precision; this guard is cheap and keeps the contract precise).
    cmpl = df["Date/Time - Complete"]
    df = df[
        cmpl.notna()
        & (cmpl.dt.date >= start_d)
        & (cmpl.dt.date <= end_d)
    ]

    # Accession-prefix facility filter. Cast to str first because the
    # parquet column can come back as int / pandas StringDtype / object
    # depending on how the source file was parsed.
    prefix = _TAT_FACILITY_PREFIX.get(facility, "")
    if prefix and "Accession Nbr - Formatted" in df.columns:
        acc = df["Accession Nbr - Formatted"].astype(str)
        df = df[acc.str.startswith(prefix, na=False)]

    df = df[df["Date/Time - Drawn"].notna()]
    if df.empty:
        return _empty

    tat_seconds = (
        df["Date/Time - Complete"] - df["Date/Time - Drawn"]
    ).dt.total_seconds()
    df = df.assign(TAT_minutes=(tat_seconds / 60.0).astype(float))

    return (
        df[["Order Procedure", "Collection Priority", "TAT_minutes"]]
        .reset_index(drop=True)
    )


def get_top_procedures_by_volume(
    tat_df: pd.DataFrame, n: int = 10,
) -> list:
    """Return the top `n` `Order Procedure` names by sample count in
    `tat_df`. Used to seed the default procedure-filter selection on
    the TAT view.
    """
    if tat_df is None or tat_df.empty:
        return []
    return (
        tat_df["Order Procedure"]
        .value_counts()
        .head(n)
        .index.tolist()
    )


def build_tat_table(
    tat_df: pd.DataFrame,
    selected_procedures: list,
) -> pd.DataFrame:
    """Build a per-procedure TAT statistics table with priority-grouped
    MultiIndex columns.

    Three groups are computed for each procedure:
      • Routine  — rows where Collection Priority == 'RT'
      • Stat     — rows where Collection Priority is 'ST' or 'TS'
                   (TS = Time Study, treated as stat / urgent)
      • Combined — every row for that procedure regardless of priority

    Per group the function reports n, Mean, and % <1h (percentage of
    samples with TAT_minutes < 60). Procedures that have no rows in a
    given group get None for every stat column in that group; rendering
    displays those as "—".

    The returned DataFrame has one row per procedure in
    `selected_procedures` and a pandas MultiIndex column structure:

        Level 0:  ['Procedure',  'Routine', 'Stat', 'Combined']
        Level 1:  ['Procedure', then 'n','Mean','% <1h' x 3]

    Rows are sorted by Combined n descending; procedures with no
    Combined samples sort to the bottom.
    """
    columns = pd.MultiIndex.from_tuples(
        [("Procedure", "Procedure")]
        + [("Routine",  c) for c in _TAT_STAT_COLS]
        + [("Stat",     c) for c in _TAT_STAT_COLS]
        + [("Combined", c) for c in _TAT_STAT_COLS]
    )

    if tat_df is None or tat_df.empty or not selected_procedures:
        return pd.DataFrame(columns=columns)

    def _group_stats(group_df: pd.DataFrame) -> list:
        """Return [n, Mean, % <1h] or 3 Nones for an empty group."""
        if group_df.empty:
            return [None, None, None]
        tats = group_df["TAT_minutes"].astype(float)
        return [
            int(len(tats)),
            float(tats.mean()),
            float((tats < 60).mean() * 100.0),
        ]

    sel = tat_df[tat_df["Order Procedure"].isin(selected_procedures)]

    rows = []
    for proc in selected_procedures:
        proc_rows = sel[sel["Order Procedure"] == proc]
        rt   = proc_rows[proc_rows["Collection Priority"] == "RT"]
        stat = proc_rows[proc_rows["Collection Priority"].isin(["ST", "TS"])]
        comb = proc_rows
        rows.append(
            [proc] + _group_stats(rt) + _group_stats(stat) + _group_stats(comb)
        )

    out = pd.DataFrame(rows, columns=columns)

    # Sort by Combined n descending; rows with None n drop to the bottom.
    out = (
        out.sort_values(("Combined", "n"), ascending=False, na_position="last")
        .reset_index(drop=True)
    )
    return out
