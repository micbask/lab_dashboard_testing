import re as _re
import base64 as _b64
import io as _io
from datetime import date

import pandas as pd
import streamlit as st

from storage import load_filtered_data, get_index_hash, storage_is_configured


def normalize_name(name) -> "str | None":
    if name is None:
        return None
    _s = str(name)
    _s = ''.join(c for c in _s if c.isprintable())
    _s = _re.sub(r'\s+', ' ', _s).strip()
    if not _s or _s == "-" or _s.lower() == "nan":
        return None
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
def load_draw_data(date_str: str, view: str) -> tuple:
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

    _idx_hash = get_index_hash() if storage_is_configured() else ""
    # Pre-Analytics scopes by Date/Time - Drawn (the column the heatmap's
    # hour axis is built from). Analytics uses the default
    # date_basis="complete" so its behavior is unchanged.
    _raw = load_filtered_data(
        start_date=_start,
        end_date=_end,
        resources=(),
        exclude_procs=(),
        _index_hash=_idx_hash,
        date_basis="drawn",
    )

    _debug: dict = {
        "raw_drawn_tech": [],
        "staff_keys": [],
        "rows_before": 0,
        "rows_after": 0,
    }

    if _raw.empty or "Drawn Tech" not in _raw.columns or "Date/Time - Drawn" not in _raw.columns:
        return _empty, _debug

    _debug["raw_drawn_tech"] = (
        _raw["Drawn Tech"].dropna().astype(str).head(10).tolist()
    )

    _df = _raw[["Drawn Tech", "Date/Time - Drawn"]].copy()
    _df["_norm"] = _df["Drawn Tech"].apply(normalize_name)
    _df = _df[_df["_norm"].notna()].copy()

    _staff = load_phlebotomy_staff()
    _debug["staff_keys"] = list(_staff.keys())[:10]
    _debug["rows_before"] = len(_df)

    _df = _df[_df["_norm"].isin(_staff)].copy()
    _debug["rows_after"] = len(_df)

    if _df.empty:
        return _empty, _debug

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
        .reset_index(drop=True),
        _debug,
    )


def build_draw_pivot(
    draw_df: pd.DataFrame,
    location: str,
    shift: "str | None",
    view: str,
) -> pd.DataFrame:
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
        _n_days = max(int(_sub["draw_datetime"].dt.date.nunique()), 1)
        _pivot = _pivot / _n_days

    return _pivot
