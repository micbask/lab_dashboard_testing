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
def load_phlebotomy_staff() -> tuple:
    import requests as _req
    _repo  = st.secrets["github"]["repo"]
    _token = st.secrets["github"]["token"]
    _owner, _rname = _repo.split("/", 1)
    _headers = {
        "Authorization": f"Bearer {_token}",
        "Accept": "application/vnd.github+json",
    }
    _resp = _req.get(
        f"https://api.github.com/repos/{_owner}/{_rname}/contents/config/phlebotomy_staff.csv",
        headers=_headers, timeout=30,
    )
    if _resp.status_code == 404:
        return {}, []
    _resp.raise_for_status()
    _raw_bytes = _b64.b64decode(_resp.json()["content"].strip())
    _df = pd.read_csv(_io.BytesIO(_raw_bytes))
    _lookup: dict = {}
    _debug_raw: list = []
    for _, _row in _df.iterrows():
        _raw_name = str(_row["Drawn Tech"])
        if len(_debug_raw) < 5:
            _debug_raw.append(repr(_raw_name))
        _display = _raw_name.strip()
        _loc  = str(_row["Location"]).strip()
        _sh_raw = _row["Shift"]
        _shift  = (
            None if (pd.isna(_sh_raw) or str(_sh_raw).strip() in ("", "nan"))
            else str(_sh_raw).strip()
        )
        _key = normalize_name(_raw_name)
        if _key:
            _lookup[_key] = {
                "display_name": _display,
                "location": _loc,
                "shift": _shift,
            }
    return _lookup, _debug_raw


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
    _raw = load_filtered_data(
        start_date=_start,
        end_date=_end,
        resources=(),
        exclude_procs=(),
        _index_hash=_idx_hash,
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

    _staff, _ = load_phlebotomy_staff()
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
    _staff, _ = load_phlebotomy_staff()

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
