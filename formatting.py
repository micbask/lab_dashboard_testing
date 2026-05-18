"""
formatting.py — Shared display formatters for analytics and pre-analytics.

The analytics and pre-analytics dashboards both render TAT minutes,
percentage values, and (min, max) ranges. Previously each had its own
copy of these helpers, which is how we ended up with three different
ways to format "1 hour 5 minutes" across the codebase. Centralised
here so both modules use identical formatting and a fix lands
everywhere at once.
"""

import pandas as pd


def format_tat(minutes) -> str:
    """Format TAT minutes as 'Xh Ym' (or 'Ym' when <1h); '-' for None/NaN.

    Example: 65  → '1h 5m'   90  → '1h 30m'   45  → '45m'
    """
    if minutes is None or pd.isna(minutes):
        return "-"
    h = int(minutes // 60)
    m = int(round(minutes - h * 60))
    if h == 0:
        return f"{m}m"
    return f"{h}h {m}m"


def format_tat_compact(minutes) -> str:
    """No-space TAT format used inside narrow uniform-width Range cells.

    Same numbers as `format_tat` but with no separator between the
    hour and minute components, so values fit tighter columns.

    Example: 8 → '8m'   60 → '1h'   354 → '5h54m'   (vs format_tat's '5h 54m')

    Returns '-' for None/NaN.
    """
    if minutes is None or pd.isna(minutes):
        return "-"
    minutes = int(round(minutes))
    h = minutes // 60
    m = minutes - h * 60
    if h == 0:
        return f"{m}m"
    if m == 0:
        return f"{h}h"
    return f"{h}h{m}m"


def format_pct(pct) -> str:
    """Format a percentage value as 'XX.X%'; '-' for None/NaN."""
    if pct is None or pd.isna(pct):
        return "-"
    return f"{pct:.1f}%"


def format_range(min_v, max_v) -> str:
    """Format a (min, max) TAT pair on ONE line, e.g. '8m-5h54m'.

    No spaces — pairs the compact hour/minute format from
    `format_tat_compact` with a single bare hyphen so the value is
    as compact as possible.

    Returns '-' if either bound is missing.
    """
    if (
        min_v is None or max_v is None
        or pd.isna(min_v) or pd.isna(max_v)
    ):
        return "-"
    return f"{format_tat_compact(min_v)}-{format_tat_compact(max_v)}"


def format_hour_12h(h: int) -> str:
    """Format a 24-hour integer as a 12-hour clock label (e.g. 14 → '2:00 PM').

    Used by the sidebar hour-range slider readouts on both dashboards
    — previously each dashboard had its own nested copy of this
    function inside render_sidebar (_fmt_h / _pa_fmt_h, both
    byte-identical).
    """
    hr12 = 12 if h % 12 == 0 else h % 12
    suf  = "AM" if h < 12 else "PM"
    return f"{hr12}:00 {suf}"
