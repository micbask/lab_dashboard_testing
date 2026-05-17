import re as _re
from datetime import date

import pandas as pd
import streamlit as st

from storage import load_filtered_data


_NAME_STOPWORDS = {"jr", "sr", "ii", "iii", "iv", "md", "phd", "rn"}


def normalize_name(name) -> "str | None":
    """Canonicalize a "Last, First" tech name for cross-source matching.

    Handles the drift that silently dropped techs in earlier versions:
      • Whitespace around the comma ("Nunez , Karina" / "Nunez,Karina").
      • Trailing middle initials ("Smith, Jane M" / "Smith, Jane M.").
      • Trailing honorifics ("Smith, Jane Jr" / "Smith, Jane RN").

    Limitation: a tech with two distinct middle initials in different
    sources (e.g. "Jane M Smith" vs "Jane R Smith") would now collapse
    to the same key. We accept that risk — it's exceedingly rare in
    practice, and the alternative (silent name-mismatch drops) is
    worse for data fidelity.
    """
    if name is None:
        return None
    _s = str(name)
    _s = ''.join(c for c in _s if c.isprintable())
    _s = _re.sub(r'\s+', ' ', _s).strip()
    if not _s or _s == "-" or _s.lower() == "nan":
        return None
    if "," in _s:
        _last, _, _rest = _s.partition(",")
        _last = _last.strip()
        _rest_parts = _rest.strip().split()
        # Drop trailing tokens that look like middle initials (≤2 chars
        # after stripping a trailing dot) or known honorifics. Keep at
        # least one token so the first name survives.
        while len(_rest_parts) > 1:
            _tail = _rest_parts[-1].rstrip(".").lower()
            if len(_tail) <= 2 or _tail in _NAME_STOPWORDS:
                _rest_parts.pop()
            else:
                break
        _rest = " ".join(_rest_parts)
        _s = f"{_last}, {_rest}" if _rest else _last
    return _s.lower()


@st.cache_data(show_spinner=False, ttl=3600)
def load_phlebotomy_staff() -> dict:
    """Read the phlebotomy staff roster from the bundled CSV and return
    {normalized_name: {display_name, location, shift}}.

    The CSV is read from the local filesystem (it is committed with the
    application); the previous GitHub-API approach hit the repository's
    default branch, which does not contain config/phlebotomy_staff.csv.

    Names contain an unquoted comma ("Last, First"), so a stock pandas
    read_csv mis-parses the rows. We split each line manually: the last
    two comma-separated fields are Location and Shift; everything before
    that is the name.
    """
    import os as _os
    _path = _os.path.join(
        _os.path.dirname(_os.path.dirname(_os.path.abspath(__file__))),
        "config", "phlebotomy_staff.csv",
    )

    _lookup: dict = {}

    if not _os.path.exists(_path):
        return _lookup

    with open(_path, "r", encoding="utf-8") as _fh:
        _lines = _fh.read().splitlines()

    if not _lines:
        return _lookup

    # Skip header row.
    for _line in _lines[1:]:
        if not _line.strip():
            continue
        _parts = [p.strip() for p in _line.split(",")]
        if len(_parts) < 3:
            continue
        # Last two fields are (Location, Shift); everything before is the name.
        _shift_raw = _parts[-1]
        _loc       = _parts[-2]
        _name_parts = _parts[:-2]
        _raw_name = ", ".join(p for p in _name_parts if p != "")
        if not _raw_name:
            continue
        _shift = _shift_raw if _shift_raw not in ("", "nan") else None
        _key = normalize_name(_raw_name)
        if _key:
            _lookup[_key] = {
                "display_name": _raw_name,
                "location": _loc,
                "shift": _shift,
            }
    return _lookup


@st.cache_data(show_spinner=False, ttl=300)
def load_draw_data(date_str: str, view: str, index_hash: str = "") -> pd.DataFrame:
    """Load phlebotomy draws scoped to the selected day/month.

    `index_hash` is a plain (non-underscored) kwarg so Streamlit's
    @cache_data hashes it into the cache key. Callers MUST pass
    `get_index_hash()` so this function's cache busts when partitions
    change (otherwise users would see stale draw data for up to 5 min
    after an ingest from a separate session).
    """
    import calendar as _cal2

    _empty = pd.DataFrame(
        columns=["display_name", "location", "shift", "draw_datetime", "hour", "samples"]
    )

    if view == "Daily":
        _d = date.fromisoformat(date_str)
        _start, _end = _d, _d
    else:
        _yr, _mo = int(date_str[:4]), int(date_str[5:7])
        _start = date(_yr, _mo, 1)
        _end   = date(_yr, _mo, _cal2.monthrange(_yr, _mo)[1])

    # Pre-Analytics scopes by Date/Time - Drawn (the column the heatmap's
    # hour axis is built from). Analytics uses the default
    # date_basis="complete" so its behavior is unchanged.
    _raw = load_filtered_data(
        start_date=_start,
        end_date=_end,
        resources=(),
        exclude_procs=(),
        index_hash=index_hash,
        date_basis="drawn",
    )

    if _raw.empty or "Drawn Tech" not in _raw.columns or "Date/Time - Drawn" not in _raw.columns:
        return _empty

    _df = _raw[["Drawn Tech", "Date/Time - Drawn"]].copy()
    _df["_norm"] = _df["Drawn Tech"].apply(normalize_name)
    _df = _df[_df["_norm"].notna()].copy()

    _staff = load_phlebotomy_staff()
    _df = _df[_df["_norm"].isin(_staff)].copy()

    if _df.empty:
        return _empty

    _df["Date/Time - Drawn"] = pd.to_datetime(_df["Date/Time - Drawn"])

    _grp = (
        _df.groupby(["Drawn Tech", "Date/Time - Drawn"], as_index=False)
           .size()
           .rename(columns={"size": "samples"})
    )

    _grp["_norm"]        = _grp["Drawn Tech"].apply(normalize_name)
    _grp["display_name"] = _grp["_norm"].map(lambda k: _staff[k]["display_name"])
    _grp["location"]     = _grp["_norm"].map(lambda k: _staff[k]["location"])
    _grp["shift"]        = _grp["_norm"].map(lambda k: _staff[k]["shift"])
    _grp["draw_datetime"] = _grp["Date/Time - Drawn"]
    _grp["hour"]          = _grp["draw_datetime"].dt.hour

    return (
        _grp[["display_name", "location", "shift", "draw_datetime", "hour", "samples"]]
        .reset_index(drop=True)
    )


def build_draw_pivot(
    draw_df: pd.DataFrame,
    location: str,
    shift: "str | None",
    view: str,
    year: "int | None" = None,
    month: "int | None" = None,
) -> pd.DataFrame:
    """Pivot draws into a (tech × hour-of-day) matrix.

    For Monthly view the values are average draws per CALENDAR day in
    the selected month. Passing `year` + `month` lets the function use
    the right denominator (days-in-month); without them, it falls back
    to days-with-data, which OVERSTATES per-day averages in sparse
    months (a 30-day month with 22 active days would inflate the avg
    by ~36%).
    """
    import calendar as _calbdp

    _staff = load_phlebotomy_staff()

    _all_techs = sorted(
        info["display_name"]
        for info in _staff.values()
        if info["location"] == location and info["shift"] == shift
    )

    _hours = list(range(24))

    if draw_df.empty:
        return pd.DataFrame(0, index=_all_techs, columns=_hours)

    if shift is None:
        _sub = draw_df[draw_df["location"] == location].copy()
    else:
        _sub = draw_df[
            (draw_df["location"] == location) & (draw_df["shift"] == shift)
        ].copy()

    if _sub.empty:
        return pd.DataFrame(0, index=_all_techs, columns=_hours)

    _pivot = (
        _sub.pivot_table(
            index="display_name", columns="hour",
            values="samples", aggfunc="count", fill_value=0,
        ).reindex(index=_all_techs, columns=_hours, fill_value=0)
    )
    _pivot.index = _all_techs

    if view == "Monthly":
        if year is not None and month is not None:
            # Elapsed-days denominator: matches analytics' semantics.
            # For a fully-past month this is the full calendar count;
            # for the current month it's days-so-far so the average
            # isn't deflated against a not-yet-elapsed month-end.
            _month_start = date(year, month, 1)
            _month_end   = date(year, month, _calbdp.monthrange(year, month)[1])
            _today       = date.today()
            _eff_end     = min(_month_end, _today)
            _n_days      = max((_eff_end - _month_start).days + 1, 1)
        else:
            # Fallback: days-with-data. Slightly overstates per-day
            # averages in sparse months; only hit when caller didn't
            # pass year/month (legacy / non-Monthly paths).
            _n_days = max(int(_sub["draw_datetime"].dt.date.nunique()), 1)
        _pivot = _pivot / max(_n_days, 1)

    return _pivot


def build_draw_count_pivot(
    draw_df: pd.DataFrame,
    location: str,
    shift: "str | None",
) -> tuple:
    """Return (counts_pivot, active_days_per_tech) for a (location, shift).

    Unlike `build_draw_pivot`, this never averages — the cells are raw
    draw counts. The companion `active_days_per_tech` dict maps each
    rostered tech's display_name to the number of distinct calendar
    dates that tech had at least one draw in the slice. The dashboard
    uses these together to compute per-tech-active-day averages:
    `cell / active_days[tech]`, which is more honest than dividing
    every tech by calendar days (the latter penalises techs who
    weren't scheduled every day of the month).

    Both return values are indexed by the full staff roster for the
    (location, shift), with 0/0 entries for rostered techs who had
    no draws in this slice — same convention as `build_draw_pivot`,
    so callers can reuse the index identically.
    """
    _staff = load_phlebotomy_staff()

    _all_techs = sorted(
        info["display_name"]
        for info in _staff.values()
        if info["location"] == location and info["shift"] == shift
    )

    _hours = list(range(24))
    _empty_active_days = {t: 0 for t in _all_techs}

    if draw_df.empty:
        return (
            pd.DataFrame(0, index=_all_techs, columns=_hours),
            _empty_active_days,
        )

    if shift is None:
        _sub = draw_df[draw_df["location"] == location].copy()
    else:
        _sub = draw_df[
            (draw_df["location"] == location) & (draw_df["shift"] == shift)
        ].copy()

    if _sub.empty:
        return (
            pd.DataFrame(0, index=_all_techs, columns=_hours),
            _empty_active_days,
        )

    _counts = (
        _sub.pivot_table(
            index="display_name", columns="hour",
            values="samples", aggfunc="count", fill_value=0,
        ).reindex(index=_all_techs, columns=_hours, fill_value=0)
    )

    _active_days = (
        _sub.groupby("display_name")["draw_datetime"]
        .apply(lambda x: x.dt.date.nunique())
        .reindex(_all_techs, fill_value=0)
        .astype(int)
        .to_dict()
    )

    return _counts, _active_days
