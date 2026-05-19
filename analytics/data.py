import calendar as _cal
from datetime import date

import pandas as pd
import streamlit as st

from config import HOUR_LABELS
from storage import load_filtered_data, get_index_hash, storage_is_configured
from analytics.filters import EXCLUDED_PROCEDURES


# ════════════════════════════════════════════════════════════════════════════
# SHARED DATA LAYER
# ════════════════════════════════════════════════════════════════════════════
#
# `load_analytics_data` is the single data-scope entry point for all three
# Analytics views (Completed, In-Lab, TAT). It applies the bench-level
# resource filter and the EXCLUDED_PROCEDURES exclusion uniformly, then
# performs view-specific post-processing:
#
#   • time_basis="Completed": no extra transform.
#   • time_basis="In-Lab":   drops rows with no In-Lab timestamp and
#                            re-maps complete_date / hour to the in-lab
#                            columns so downstream pivots key off in-lab
#                            time.
#   • time_basis="TAT":      parses Complete + Drawn timestamps, drops
#                            rows with null Drawn, computes TAT_minutes.
#
# Volume aggregation (build_pivot, build_monthly_pivot, build_weekday_pivot)
# and TAT tabulation (build_tat_table) live downstream — they receive
# already-scoped DataFrames from this loader.
#
# Pre-refactor history: Completed/In-Lab called load_filtered_data
# directly with an inline in-lab remap in analytics/dashboard.py; TAT
# went through a separate `load_tat_data` that bypassed
# EXCLUDED_PROCEDURES (passing exclude_procs=()) and applied an
# accession-prefix facility filter. Both paths converged here so that
# TAT reports turnaround on the same procedure universe Completed and
# In-Lab show.

@st.cache_data(show_spinner=False, ttl=300)
def load_analytics_data(
    *,
    start_date: date,
    end_date: date,
    resources: tuple,
    time_basis: str,
    index_hash: str = "",
) -> pd.DataFrame:
    """Load Analytics data scoped to a Testing Bench + date range, with
    per-view post-processing applied.

    Parameters
    ----------
    start_date, end_date : date
        Inclusive date range. Partition-pruned at the parquet layer.
    resources : tuple[str, ...]
        Performing Service Resource values for the selected Testing
        Bench (e.g. `DEFAULT_RESOURCES["Keck Core"]`).
    time_basis : {"Completed", "In-Lab", "TAT"}
        Drives the post-load transform; see module docstring.
    index_hash : str
        Partition-index hash for cache invalidation. Passed through
        from `storage.get_index_hash()`. Plain name (no leading `_`) so
        Streamlit's @cache_data hashes it into the cache key —
        underscored names are skipped, which was silently serving stale
        cross-session data in a prior version.

    Returns
    -------
    pd.DataFrame
        Scoped DataFrame ready for downstream metric functions.
        Returns an empty DataFrame when storage is not configured
        or when the date/bench combination has no matching rows.
    """
    if not storage_is_configured():
        return pd.DataFrame()

    # For In-Lab, query the partition store by `inlab_date` so we load
    # the rows whose IN-LAB happened in [start_date, end_date], not the
    # rows whose COMPLETION happened in that window. Previously we
    # loaded by complete_date and then remapped — that under-counted
    # rows that arrived in-lab on the target date but completed the
    # next day, and over-counted rows that completed on the target
    # date but actually arrived in-lab the previous day. Both errors
    # are silently rolled into the KPIs (Total volume / Avg per hour)
    # which made In-Lab and Completed views report identical totals
    # any time the row set "happened" to overlap.
    df = load_filtered_data(
        start_date=start_date,
        end_date=end_date,
        resources=resources,
        exclude_procs=tuple(sorted(EXCLUDED_PROCEDURES)),
        index_hash=index_hash,
        date_basis="inlab" if time_basis == "In-Lab" else "complete",
    )
    if df.empty:
        return df

    if time_basis == "In-Lab":
        # The load already restricted rows to inlab_date in
        # [start_date, end_date]. Drop the rare row that lacks an
        # inlab_hour (NaT) and would explode the hour-bucket dtype
        # at .astype(int) below, then remap the `complete_date` /
        # `hour` helper columns to the in-lab values so downstream
        # pivots (which key off `complete_date` / `hour`) bucket by
        # in-lab time without further parameterisation.
        if (
            "inlab_date" not in df.columns
            or not df["inlab_date"].notna().any()
        ):
            # Preserve a non-empty DataFrame skeleton with the same
            # columns so callers can distinguish "no In-Lab data" from
            # "no rows at all" by checking df.empty after filtering.
            return df.iloc[0:0]
        df = df[df["inlab_date"].notna() & df["inlab_hour"].notna()].copy()
        df["complete_date"] = df["inlab_date"]
        # inlab_hour is nullable Int64 — go through Int64 first so any
        # residual <NA> from upstream dtype drift surfaces here as a
        # filter (above) rather than as an AttributeError at .astype(int).
        df["hour"] = df["inlab_hour"].astype("Int64").astype(int)
        return df

    if time_basis == "TAT":
        # TAT needs both timestamps to compute (Complete - Drawn). Filter
        # rows missing either, and discard negative TATs (data-entry
        # errors where Drawn > Complete) - those would otherwise drag
        # the mean down and incorrectly count as "% under 1 hour".
        if "Date/Time - Drawn" not in df.columns:
            return df.iloc[0:0]
        df = df.copy()
        df["Date/Time - Complete"] = pd.to_datetime(
            df["Date/Time - Complete"], errors="coerce"
        )
        df["Date/Time - Drawn"] = pd.to_datetime(
            df["Date/Time - Drawn"], errors="coerce"
        )
        df = df[
            df["Date/Time - Drawn"].notna()
            & df["Date/Time - Complete"].notna()
        ]
        if df.empty:
            return df
        tat_seconds = (
            df["Date/Time - Complete"] - df["Date/Time - Drawn"]
        ).dt.total_seconds()
        df = df.assign(TAT_minutes=(tat_seconds / 60.0).astype(float))
        df = df[df["TAT_minutes"] >= 0]
        return df

    # time_basis == "Completed" — return as-is.
    return df


def build_pivot(
    df: pd.DataFrame,
    selected_date: date,
    hour_range: tuple,
    top_n: int | None = 30,
) -> tuple:
    """Build the Daily heatmap pivot.

    `top_n` controls how many top-volume procedures the heatmap shows:
        - int  -> keep the top-N procedures by full-day total volume
        - None -> keep ALL procedures (the sidebar's "All" option)
    """
    h_start, h_end = hour_range
    hours   = list(range(h_start, h_end + 1))
    df_date = df[df["complete_date"] == selected_date].copy()
    df_dh   = df_date[df_date["hour"].isin(hours)].copy()

    if df_dh.empty:
        return None, None, df_date, hours

    if top_n is not None:
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
    top_n: int | None = 30,
) -> tuple:
    """Build the Monthly heatmap pivot.

    `top_n` controls how many top-volume procedures the heatmap shows:
        • int  — keep the top-N procedures by month total volume
        • None — keep ALL procedures (the sidebar's "All" option)
    """
    month_start = date(year, month, 1)
    month_end   = date(year, month, _cal.monthrange(year, month)[1])
    month_df    = df[
        (df["complete_date"] >= month_start) &
        (df["complete_date"] <= month_end)
    ].copy()

    if month_df.empty:
        return None, 0, month_df

    if top_n is not None:
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

    # n_days = days the user expects data for in the displayed period,
    # NOT just days that happened to have data. Using nunique() of
    # observed dates inflates the per-day average whenever a weekend
    # or holiday had no data (Saturday with zero draws should pull the
    # average DOWN, not be excluded from the denominator). For partial
    # current months we cap at today so the average reflects elapsed
    # days only — calendar-days-to-month-end would deflate the
    # average for a mid-month view.
    _today = date.today()
    _effective_end = min(month_end, _today)
    if _effective_end < month_start:
        n_days = 1  # future month — defensive denominator
    else:
        n_days = (_effective_end - month_start).days + 1
    avg = pivot / n_days
    avg["Total"] = avg.sum(axis=1)
    avg = avg.sort_values("Total", ascending=False)
    avg.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in avg.columns]
    return avg, n_days, month_df


@st.cache_data(show_spinner=False, ttl=300)
def build_weekday_pivot(
    _month_df: pd.DataFrame, year: int, month: int,
    cache_key: str = "",
) -> tuple:
    """Build the Monthly weekday-pattern pivot.

    `_month_df` is underscore-prefixed so Streamlit skips hashing the
    DataFrame (would be expensive for a month-sized frame). `cache_key`
    carries the (map_type, time_basis, idx_hash) fingerprint that
    callers must supply so the cache busts when scope changes.
    """
    _ = cache_key  # consumed by st.cache_data for cache-key fingerprinting
    if _month_df.empty:
        return None, {}

    df = _month_df.copy()
    _complete_dt = pd.to_datetime(df["complete_date"])
    df["weekday"]   = _complete_dt.dt.dayofweek
    df["date_only"] = _complete_dt.dt.date

    pivot = (
        df.pivot_table(
            index="weekday", columns="hour",
            values="Complete Volume", aggfunc="sum", fill_value=0,
        ).reindex(index=list(range(7)), columns=list(range(24)), fill_value=0)
    )

    # Divisor = count of DISTINCT DATES per weekday that actually
    # appear in the data, not the count of calendar weekdays in the
    # month. The calendar-count approach (previously used here) was
    # wrong on partial months: in May 2026 with data through May 18,
    # the month has 4 calendar Mondays (May 4/11/18/25) but only ~2
    # have actually elapsed with data, so dividing accumulated
    # Monday draws by 4 silently deflated the per-Monday average by
    # ~2×. Matching the pre-analytics divisor here is also robust to
    # mid-month data gaps (uploaded data missing a Wednesday is
    # treated correctly — divisor drops by 1 instead of still using
    # the calendar count). `year`/`month` no longer needed for the
    # divisor but kept in the signature for cache-key compatibility
    # and to keep the call sites unchanged.
    _ = (year, month)
    wd_counts: dict = {
        wd: int(df.loc[df["weekday"] == wd, "date_only"].nunique())
        for wd in range(7)
    }

    for wd in range(7):
        pivot.loc[wd] = pivot.loc[wd] / max(wd_counts[wd], 1)

    pivot["Total"] = pivot[list(range(24))].sum(axis=1)

    _day_names = ["Monday", "Tuesday", "Wednesday", "Thursday",
                  "Friday", "Saturday", "Sunday"]
    pivot.index = [f"{_day_names[wd]}  (×{wd_counts[wd]})" for wd in range(7)]
    pivot.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in pivot.columns]
    return pivot, wd_counts


@st.cache_data(show_spinner=False, ttl=300)
def load_monthly_avg_for_comparison(
    _df: pd.DataFrame,
    year: int,
    month: int,
    cache_key: str = "",
) -> pd.DataFrame:
    """Build the procedure × hour-of-day average-per-day pivot for the
    month identified by (year, month).

    Used by the Daily heatmap's hover tooltip in analytics/dashboard.py
    to compare today's count to the month's per-day average for the
    same (procedure, hour) cell. The returned DataFrame is indexed by
    procedure (the FULL set present in the month, not pre-filtered)
    with integer hour columns 0..23.

    Caching: keyed on (year, month, cache_key). The pivot is the same
    for every day within the month and for every procedure-selection
    the caller might make, so one cache entry is reused across all
    day-navigations within the month — previously this function keyed
    by `selected_date` + the per-day procedure tuple, so each day
    produced its own cache entry. Callers filter the returned pivot
    to their displayed procedures.

    Returns an empty DataFrame if `_df` has no rows in the month.

    `_df` is underscore-prefixed so Streamlit skips hashing it.
    `cache_key` carries the (map_type, time_basis, idx_hash) fingerprint.
    """
    _ = cache_key  # consumed by st.cache_data for cache-key fingerprinting
    if _df is None or _df.empty:
        return pd.DataFrame()

    month_start = date(year, month, 1)
    month_end   = date(year, month, _cal.monthrange(year, month)[1])

    month_df = _df[
        (_df["complete_date"] >= month_start) &
        (_df["complete_date"] <= month_end)
    ]
    if month_df.empty:
        return pd.DataFrame()

    pivot = (
        month_df.pivot_table(
            index="Order Procedure", columns="hour",
            values="Complete Volume", aggfunc="sum", fill_value=0,
        ).reindex(columns=list(range(24)), fill_value=0)
    )
    # Match build_monthly_pivot's denominator semantics — elapsed days
    # in the displayed period, not days with any data. See the
    # comment in build_monthly_pivot for the rationale.
    _today = date.today()
    _effective_end = min(month_end, _today)
    if _effective_end < month_start:
        n_days = 1
    else:
        n_days = (_effective_end - month_start).days + 1
    return pivot / n_days


# ════════════════════════════════════════════════════════════════════════════
# TAT METRIC LAYER
# ════════════════════════════════════════════════════════════════════════════

# TAT targets now live in config.SITE_CONFIG (per bench) with
# DEFAULT_TAT_TARGETS as the fallback. Re-exported here for callers
# that import from analytics.data — adding a bench-specific TAT
# override is a one-line edit in config.SITE_CONFIG.
from config import (
    DEFAULT_TAT_TARGETS as TAT_TARGET_MINUTES,
    TAT_TARGET_OVERRIDES,
    get_tat_targets,
)

# Stat columns reported per priority group inside `build_tat_table`'s
# MultiIndex output. The order here is the order they appear in the
# rendered table.
_TAT_STAT_COLS = ["n", "Mean", "% within target", "Min", "Max"]


def compute_tat_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Project a `load_analytics_data(time_basis='TAT')` result down to
    the 3-column shape that `build_tat_table` and the TAT KPI strip
    consume.

    The shared loader has already:
      • applied bench + EXCLUDED_PROCEDURES + date scoping,
      • parsed Complete + Drawn timestamps,
      • dropped rows with null Drawn,
      • computed TAT_minutes = (Complete - Drawn) / 60.

    This function selects the three columns the TAT view needs.
    Returns an empty 3-column DataFrame when the input is missing
    the expected columns (e.g. storage not configured) so callers
    can safely `.empty`-check the result.
    """
    if df is None or df.empty or "TAT_minutes" not in df.columns:
        return pd.DataFrame(
            columns=["Order Procedure", "Collection Priority", "TAT_minutes"]
        )
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
    targets: dict[str, int],
) -> pd.DataFrame:
    """Build a per-procedure TAT statistics table with priority-grouped
    MultiIndex columns.

    Four groups are computed for each procedure:
      • RT  — Collection Priority == 'RT'   (Routine, target ≤ 2h)
      • ST  — Collection Priority == 'ST'   (Stat,    target ≤ 1h)
      • TS  — Collection Priority == 'TS'   (Time Study, target ≤ 1h)
      • All — every row regardless of priority (weighted % within
              target: each sample evaluated against its own priority's
              threshold from TAT_TARGET_MINUTES)

    Each group reports n, Mean, % within target, Min, and Max. The
    dashboard renders Min/Max as a combined "Range" column. Procedures
    with no rows in a given group get None for every stat column in
    that group; rendering displays those as "-".

    The returned DataFrame has one row per procedure in
    `selected_procedures` and a pandas MultiIndex column structure:

        Level 0:  ['Procedure', 'RT', 'ST', 'TS', 'All']
        Level 1:  ['Procedure', then 'n', 'Mean', '% within target',
                   'Min', 'Max' x 4]

    Row order PRESERVES `selected_procedures` order so callers can
    control the display sequence (e.g. fixed clinical-priority order
    for core-panel defaults vs descending-by-volume for the top-N
    default). The historic descending-by-(All, n) sort was removed
    because it forced every default into volume order.
    """
    columns = pd.MultiIndex.from_tuples(
        [("Procedure", "Procedure")]
        + [("RT",  c) for c in _TAT_STAT_COLS]
        + [("ST",  c) for c in _TAT_STAT_COLS]
        + [("TS",  c) for c in _TAT_STAT_COLS]
        + [("All", c) for c in _TAT_STAT_COLS]
    )

    if tat_df is None or tat_df.empty or not selected_procedures:
        return pd.DataFrame(columns=columns)

    _targets = targets

    def _group_stats(group_df: pd.DataFrame, threshold_minutes: int) -> list:
        """Return [n, Mean, % within target, Min, Max] for a
        single-priority subset evaluated against `threshold_minutes`.
        Five Nones for an empty subset."""
        if group_df.empty:
            return [None, None, None, None, None]
        tats = group_df["TAT_minutes"].astype(float)
        return [
            int(len(tats)),
            float(tats.mean()),
            float((tats <= threshold_minutes).mean() * 100.0),
            float(tats.min()),
            float(tats.max()),
        ]

    def _all_stats(proc_rows: pd.DataFrame) -> list:
        """Return [n, Mean, % within target, Min, Max] for the All
        aggregate.

        % within target uses each sample's priority-specific threshold
        via vectorized lookup against the resolved `_targets` dict.
        Samples whose Collection Priority isn't in `_targets` (rare
        but real — odd values like 'NU', blanks, lowercase variants)
        are EXCLUDED from the % denominator instead of being silently
        counted as "missed target", which would falsely depress the
        compliance rate. n and Mean still reflect every sample so the
        volume reported matches the data.
        """
        if proc_rows.empty:
            return [None, None, None, None, None]
        tats = proc_rows["TAT_minutes"].astype(float)
        n = int(len(proc_rows))
        mean = float(tats.mean())
        known_mask = proc_rows["Collection Priority"].isin(_targets)
        if known_mask.any():
            known_tats = tats[known_mask]
            thresholds = proc_rows.loc[known_mask, "Collection Priority"].map(_targets).astype(float)
            meets = int((known_tats <= thresholds).sum())
            pct = float(meets / int(known_mask.sum()) * 100.0)
        else:
            pct = None
        return [
            n,
            mean,
            pct,
            float(tats.min()),
            float(tats.max()),
        ]

    sel = tat_df[tat_df["Order Procedure"].isin(selected_procedures)]

    rows = []
    for proc in selected_procedures:
        proc_rows = sel[sel["Order Procedure"] == proc]
        rt = proc_rows[proc_rows["Collection Priority"] == "RT"]
        st = proc_rows[proc_rows["Collection Priority"] == "ST"]
        ts = proc_rows[proc_rows["Collection Priority"] == "TS"]

        rows.append(
            [proc]
            + _group_stats(rt, _targets["RT"])
            + _group_stats(st, _targets["ST"])
            + _group_stats(ts, _targets["TS"])
            + _all_stats(proc_rows)
        )

    # Row order = selected_procedures order, NOT sorted by volume.
    # Callers control the sequence by passing procedures in their
    # desired display order.
    return pd.DataFrame(rows, columns=columns)
