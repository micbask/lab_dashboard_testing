"""
app.py — Lab Productivity Heatmap Dashboard (Orchestrator)
Keck Medicine of USC

CRITICAL DATA ACCESS PATTERN:
  The dashboard NEVER loads the full dataset into memory.
  - Sidebar metadata comes from the partition index (a tiny JSON file).
  - Heatmap data comes from load_filtered_data() which reads ONLY the
    partition(s) covering the selected date/month, filtered by resources
    and procedure exclusions via DuckDB.
  - No fresh_df in session state — after writes, we invalidate the cache
    and let the next render re-read only what it needs.
"""

import calendar as _cal
from copy import deepcopy
from datetime import date, timedelta

import altair as alt
import pandas as pd
import streamlit as st

from config import (
    DEFAULT_RESOURCES, VMAX, ALL_RESOURCES, MAP_TYPES,
    EXCLUDE_PROCS, HOUR_LABELS, LABEL_TO_HOUR,
)
from parsing import parse_single_file, deduplicate_and_merge, clean_procedure_names
from storage import (
    storage_is_configured, get_data_summary,
    load_filtered_data, ingest_new_data,
    delete_date_range, reset_all_data,
    ensure_partitioned_storage, get_index_hash,
    count_rows_in_date_range,
)
from forecasting import (
    load_forecasts, retrain_all_forecasts_streaming,
    build_forecast_pivot,
)
from ui_components import (
    inject_css, setup_mpl_font,
    metric_card, render_header, status_chip,
    style_pivot, style_monthly_pivot, style_forecast_pivot,
    build_png, build_monthly_png, build_weekday_png,
)


# ═════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG  (must be the first Streamlit call)
# ═════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Lab Productivity · Keck Medicine",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)

inject_css()
setup_mpl_font()


# ═════════════════════════════════════════════════════════════════════════════
# PASSWORD GATE
# ═════════════════════════════════════════════════════════════════════════════
_app_password = st.secrets.get("app_password", None)

if "app_authenticated" not in st.session_state:
    st.session_state["app_authenticated"] = False

if _app_password is not None and not st.session_state["app_authenticated"]:
    st.markdown('<div id="login-overlay">', unsafe_allow_html=True)
    st.markdown('<div style="height: 15vh; min-height: 48px;"></div>',
                unsafe_allow_html=True)
    _, col, _ = st.columns([1, 0.9, 1])
    with col:
        st.markdown("""
            <div style="
                background: linear-gradient(150deg, #6F1828 0%, #521322 60%, #3d0e19 100%);
                padding: 2rem 2.4rem 1.8rem 2.4rem;
                border-radius: 12px 12px 0 0;
                text-align: center;
                box-shadow: none;
            ">
                <div style="
                    display: inline-block;
                    background: rgba(237,193,83,0.18);
                    border: 1px solid rgba(237,193,83,0.4);
                    color: #EDC153;
                    font-family: 'Inter', system-ui, sans-serif;
                    font-size: 0.62rem;
                    font-weight: 700;
                    letter-spacing: 0.18em;
                    text-transform: uppercase;
                    padding: 3px 12px;
                    border-radius: 20px;
                    margin-bottom: 1rem;
                ">CLINICAL LAB</div>
                <div style="
                    color: #ffffff;
                    font-family: 'Inter', system-ui, sans-serif;
                    font-size: 1.45rem;
                    font-weight: 700;
                    letter-spacing: 0.01em;
                    margin: 0 0 0.3rem 0;
                    line-height: 1.2;
                ">Productivity Dashboard</div>
                <div style="
                    color: rgba(237,193,83,0.85);
                    font-family: 'Inter', system-ui, sans-serif;
                    font-size: 0.82rem;
                    font-weight: 400;
                    letter-spacing: 0.01em;
                    margin: 0;
                ">Analytics</div>
            </div>
            <div style="
                background: #ffffff;
                padding: 1.8rem 2.4rem 2.2rem 2.4rem;
                border-radius: 0 0 12px 12px;
                border: 1px solid #dde1e7;
                border-top: none;
                box-shadow: 0 8px 32px rgba(0,0,0,0.12);
            ">
                <p style="
                    color: #475569;
                    font-size: 0.82rem;
                    margin: 0 0 1.2rem 0;
                    font-family: 'Inter', system-ui, sans-serif;
                ">Enter your access password to continue.</p>
        """, unsafe_allow_html=True)
        with st.form("login_form", enter_to_submit=True):
            password = st.text_input("Password", type="password",
                                     label_visibility="collapsed",
                                     placeholder="Password")
            submitted = st.form_submit_button("Sign In",
                                              width="stretch")
            if submitted:
                if password == st.secrets.get("app_password", ""):
                    st.session_state["app_authenticated"] = True
                    st.rerun()
                else:
                    st.error("Incorrect password. Please try again.")
        st.markdown("""
            </div>
            <div style="text-align:center; margin-top: 1.2rem; color: #94a3b8; font-size: 0.7rem; font-family: 'Inter', system-ui, sans-serif;">
                Laboratory &nbsp;·&nbsp; Dashboard
            </div>
        """, unsafe_allow_html=True)
    st.markdown('</div>', unsafe_allow_html=True)
    st.stop()


# ═════════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALISATION
# ═════════════════════════════════════════════════════════════════════════════
_ss = st.session_state

if "resource_assignments" not in _ss:
    _ss.resource_assignments = deepcopy(DEFAULT_RESOURCES)
if "last_map_type" not in _ss:
    _ss.last_map_type = None

_active_dashboard = st.query_params.get("dashboard", "analytics")


# ═════════════════════════════════════════════════════════════════════════════
# LOCAL HELPER FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════

def build_pivot(
    df: pd.DataFrame,
    selected_date: date,
    hour_range: tuple[int, int],
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
    wd_counts: dict[int, int] = {wd: 0 for wd in range(7)}
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


# ═════════════════════════════════════════════════════════════════════════════
# ── PRE-ANALYTICS DATA LAYER ──
# ═════════════════════════════════════════════════════════════════════════════

import re as _re
import base64 as _b64
import io as _io

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
    from storage import load_filtered_data, get_index_hash
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
        _df.groupby(["_norm", "Date/Time - Drawn"], as_index=False)
           .size()
           .rename(columns={"size": "samples"})
    )

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


# ═════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═════════════════════════════════════════════════════════════════════════════
with st.sidebar:
    if _active_dashboard == "analytics":

        st.markdown("### Map Type")
        map_type = st.selectbox("Map type", MAP_TYPES, label_visibility="collapsed")

        if _ss.last_map_type != map_type:
            _ss.pop("date_picker", None)
            _ss.last_map_type = map_type

        st.markdown("### View")
        view_mode = st.radio(
            "View", ["Daily", "Monthly"],
            horizontal=True, label_visibility="collapsed",
        )

        st.markdown("### Time Basis")
        time_basis = st.radio(
            "Time Basis", ["Completed", "In-Lab"],
            horizontal=True, label_visibility="collapsed",
        )

        if _ss.pop("pending_forecast_retrain", False):
            if storage_is_configured():
                with st.spinner("Retraining forecast models…"):
                    retrain_all_forecasts_streaming(_ss.resource_assignments)
                st.success("Forecast models retrained.")
            else:
                st.warning("No storage configured — cannot retrain forecasts.")

        if _ss.pop("pending_reset", False):
            try:
                if storage_is_configured():
                    reset_all_data()
                    st.cache_data.clear()
                    st.success("Master dataset cleared.")
                for _mt in MAP_TYPES:
                    _ss.pop(f"forecasts_{_mt}", None)
            except Exception as _rst_err:
                st.error(f"Reset failed: {_rst_err}")

        if "pending_delete_range" in _ss:
            del_info = _ss.pop("pending_delete_range")
            try:
                result = delete_date_range(del_info["start"], del_info["end"])
                st.cache_data.clear()
                st.success(
                    f"Deleted {result['rows_removed']:,} rows "
                    f"({del_info['start']} → {del_info['end']})."
                )
            except Exception as _dr_err:
                st.error(f"Delete failed: {_dr_err}")

        st.markdown("---")
        st.markdown("### Data Source")

        _data_exists = False
        _data_summary = {"total_rows": 0, "partitions": 0}

        if storage_is_configured():
            st.caption("Storage: GitHub (partitioned)")

            try:
                _data_exists = ensure_partitioned_storage()
                if _data_exists:
                    _data_summary = get_data_summary()
                    if _data_summary["total_rows"] == 0:
                        _data_exists = False

                if _data_exists:
                    status_chip(
                        f"{_data_summary['total_rows']:,} rows · "
                        f"{_data_summary['min_date']} → {_data_summary['max_date']}",
                        level="ok",
                    )
                else:
                    status_chip("No data yet — upload below", level="warn")

            except Exception as _load_err:
                status_chip("Load error", level="error")
                st.error(f"Could not read data index: {_load_err}")

            st.markdown("---")
            with st.expander("Data Management", expanded=not _data_exists):

                admin_pw   = st.secrets.get("admin_password", None)
                authorized = True
                if admin_pw:
                    if _ss.get("admin_authorized", False):
                        authorized = True
                    else:
                        entered_pw = st.text_input(
                            "Admin password", type="password", key="admin_pw"
                        )
                        if entered_pw == admin_pw:
                            _ss["admin_authorized"] = True
                            st.rerun()
                        elif entered_pw:
                            st.error("Incorrect password.")
                        authorized = _ss.get("admin_authorized", False)

                if authorized:

                    st.markdown('<div class="refresh-btn">', unsafe_allow_html=True)
                    if st.button("↺  Refresh data", width="stretch", key="refresh_data_btn"):
                        st.cache_data.clear()
                        _ss.pop("_partition_index", None)
                        st.rerun()
                    st.markdown('</div>', unsafe_allow_html=True)

                    if st.button(
                        "⟳  Refresh Forecast", width="stretch",
                        key="refresh_forecast_btn",
                        disabled=(not _data_exists),
                    ):
                        _ss["pending_forecast_retrain"] = True
                        st.rerun()

                    if _data_exists:
                        st.markdown("**Current dataset**")
                        st.caption(
                            f"Rows: **{_data_summary['total_rows']:,}**  \n"
                            f"Date range: **{_data_summary['min_date']}** → "
                            f"**{_data_summary['max_date']}**  \n"
                            f"Partitions: **{_data_summary['partitions']}**"
                        )

                        with st.expander("Remove a date range", expanded=False):
                            st.caption(
                                "Permanently deletes all rows in the chosen window "
                                "from the master dataset.  This cannot be undone."
                            )
                            _dr_min = date.fromisoformat(_data_summary["min_date"])
                            _dr_max = date.fromisoformat(_data_summary["max_date"])
                            _dc1, _dc2 = st.columns(2)
                            with _dc1:
                                del_start = st.date_input(
                                    "From", value=_dr_min,
                                    min_value=_dr_min, max_value=_dr_max,
                                    key="del_start",
                                )
                            with _dc2:
                                del_end = st.date_input(
                                    "To", value=_dr_min,
                                    min_value=_dr_min, max_value=_dr_max,
                                    key="del_end",
                                )
                            if del_start > del_end:
                                st.error("'From' date must be on or before 'To' date.")
                            else:
                                _affected = count_rows_in_date_range(del_start, del_end)
                                if _affected:
                                    st.warning(f"Will delete **{_affected:,}** rows.")

                            if st.button(
                                "Delete this range", type="primary",
                                width="stretch", key="btn_del_range",
                                disabled=(del_start > del_end),
                            ):
                                _ss["pending_delete_range"] = {
                                    "start": del_start, "end": del_end
                                }
                                st.rerun()

                        st.markdown("---")

                    st.markdown("**Step 1 — Select file(s)**")
                    new_files = st.file_uploader(
                        "Upload files", type=["xlsx", "xls", "csv"],
                        key="admin_upload",
                        label_visibility="collapsed",
                        accept_multiple_files=True,
                    )
                    if new_files:
                        _staged = []
                        for _uf in new_files:
                            _staged.append({"bytes": _uf.read(), "name": _uf.name})
                        _ss["staged_files"] = _staged
                        _names = ", ".join(f"**{s['name']}**" for s in _staged)
                        st.caption(f"Ready: {_names}  ({len(_staged)} file(s))")

                        st.markdown("**Step 2 — Add to master dataset**")
                        if st.button(
                            "Process & add to master",
                            type="primary", width="stretch",
                        ):
                            _ss["pending_upload"] = _ss.pop("staged_files")
                            st.rerun()

                    st.markdown("---")
                    st.markdown("**Danger zone**")
                    if st.button(
                        "Reset — delete all data", width="stretch",
                    ):
                        _ss["pending_reset"] = True
                        st.rerun()

        else:
            st.markdown("**Data source:** Local file upload")
            st.caption(
                "Configure GitHub in Streamlit secrets to enable persistent storage."
            )
            uploaded_files = st.file_uploader(
                "Upload Excel file(s)", type=["xlsx", "xls", "csv"],
                accept_multiple_files=True,
            )
            _local_df = pd.DataFrame()
            merge_log = []
            if uploaded_files:
                _parsed = []
                for f in uploaded_files:
                    _df = parse_single_file(f.read(), filename=f.name)
                    if not _df.empty:
                        _parsed.append((f.name, _df))
                if _parsed:
                    _local_df, merge_log = deduplicate_and_merge(_parsed)
                    _data_exists = not _local_df.empty

        st.markdown("---")

        if _data_exists:
            _current_resources = tuple(sorted(_ss.resource_assignments[map_type]))
            _current_excludes = tuple(sorted(EXCLUDE_PROCS))
            _idx_hash = get_index_hash() if storage_is_configured() else ""

            if storage_is_configured():
                _min_d = date.fromisoformat(_data_summary["min_date"])
                _max_d = date.fromisoformat(_data_summary["max_date"])
            else:
                _min_d = _local_df["complete_date"].min()
                _max_d = _local_df["complete_date"].max()

            if view_mode == "Daily":
                _fc_data = load_forecasts(map_type)
                forecast_dates: list = []
                _fc_max_d = _max_d
                if _fc_data:
                    _fc_start_d = _max_d + timedelta(days=1)
                    _fc_end_d   = _fc_data["forecast_end"]
                    forecast_dates = [
                        _fc_start_d + timedelta(days=_i)
                        for _i in range((_fc_end_d - _fc_start_d).days + 1)
                    ]
                    _fc_max_d = _fc_end_d

                if "_pending_date" in _ss:
                    _pending = _ss.pop("_pending_date")
                    if _min_d <= _pending <= _fc_max_d:
                        _ss["date_picker"] = _pending

                if (
                    "date_picker" not in _ss
                    or _ss["date_picker"] < _min_d
                    or _ss["date_picker"] > _fc_max_d
                ):
                    _ss["date_picker"] = _max_d

                st.markdown("### Date")
                _fc_note = (
                    f"  +  forecast to **{_fc_max_d}**" if forecast_dates else ""
                )
                st.caption(f"{_min_d} → {_max_d}{_fc_note}")

                picked_date = st.date_input(
                    "Select date",
                    min_value=_min_d,
                    max_value=_fc_max_d,
                    label_visibility="collapsed",
                    key="date_picker",
                )
                selected_date = picked_date

                _is_forecast_date = selected_date > _max_d

                _nc1, _nc2 = st.columns(2)
                with _nc1:
                    if st.button(
                        "◄ Prev", width="stretch",
                        disabled=(selected_date <= _min_d),
                    ):
                        _ss["_pending_date"] = selected_date - timedelta(days=1)
                        st.rerun()
                with _nc2:
                    if st.button(
                        "Next ►", width="stretch",
                        disabled=(selected_date >= _fc_max_d),
                    ):
                        _ss["_pending_date"] = selected_date + timedelta(days=1)
                        st.rerun()

                if _fc_data:
                    _fc_trained_on = _fc_data.get("last_data_date")
                    if _fc_trained_on is not None and _fc_trained_on != _max_d:
                        st.markdown(
                            '<div style="background:#7a3800;color:#FFE0B2;padding:0.5rem 0.7rem;'
                            'border-radius:6px;font-size:0.78rem;margin-top:0.3rem;">'
                            "⚠ Forecast is out of date. Use the <strong>Refresh Forecast</strong> "
                            "button in Data Management to retrain."
                            "</div>",
                            unsafe_allow_html=True,
                        )
                else:
                    st.caption("No forecast available — use Refresh Forecast to generate one.")

                st.markdown("---")
                st.markdown("### Hour Range")

                def _fmt_h(h: int) -> str:
                    hr12 = 12 if h % 12 == 0 else h % 12
                    suf  = "AM" if h < 12 else "PM"
                    return f"{hr12}:00 {suf}"

                hour_range = st.slider(
                    "Hours", 0, 23, (0, 23), label_visibility="collapsed"
                )
                st.caption(f"{_fmt_h(hour_range[0])} → {_fmt_h(hour_range[1])}")

            else:
                import itertools
                _avail_months = []
                d = date(_min_d.year, _min_d.month, 1)
                end_m = date(_max_d.year, _max_d.month, 1)
                while d <= end_m:
                    _avail_months.append((d.year, d.month))
                    if d.month == 12:
                        d = date(d.year + 1, 1, 1)
                    else:
                        d = date(d.year, d.month + 1, 1)

                if not _avail_months:
                    st.warning("No data found for this map type.")
                    st.stop()

                _month_labels = [
                    f"{_cal.month_name[m]} {y}" for y, m in _avail_months
                ]

                st.markdown("### Month")
                _sel_month_label = st.selectbox(
                    "Select month",
                    _month_labels,
                    index=len(_avail_months) - 1,
                    label_visibility="collapsed",
                )
                _sel_idx = _month_labels.index(_sel_month_label)
                selected_year, selected_month = _avail_months[_sel_idx]

            st.markdown("---")
            with st.expander("Resource Allocation", expanded=False):
                st.markdown(
                    "Reassign instruments between maps. "
                    "Each resource should appear in only one map."
                )
                new_assignments = {}
                for mt in MAP_TYPES:
                    new_assignments[mt] = st.multiselect(
                        mt, options=ALL_RESOURCES,
                        default=_ss.resource_assignments.get(mt, []),
                        key=f"res_{mt}",
                    )
                _flat  = [r for rs in new_assignments.values() for r in rs]
                _dupes = sorted({r for r in _flat if _flat.count(r) > 1})
                if _dupes:
                    st.warning(f"Duplicate assignments: {', '.join(_dupes)}")

                _ra, _rb = st.columns(2)
                with _ra:
                    if st.button("Apply", width="stretch", type="primary"):
                        _ss.resource_assignments = new_assignments
                        st.cache_data.clear()
                        st.rerun()
                with _rb:
                    if st.button("Reset defaults", width="stretch"):
                        _ss.resource_assignments = deepcopy(DEFAULT_RESOURCES)
                        st.cache_data.clear()
                        st.rerun()


    else:
        st.markdown("### Location")
        pa_location = st.radio(
            "Location", ["Keck", "Norris", "HC3"],
            horizontal=True, label_visibility="collapsed",
            key="pa_location_radio",
        )

        st.markdown("### View")
        pa_view = st.radio(
            "View", ["Daily", "Monthly"],
            horizontal=True, label_visibility="collapsed",
            key="pa_view_radio",
        )

        st.markdown("---")

        _pa_data_ok = False
        _pa_min_d = date.today() - timedelta(days=30)
        _pa_max_d = date.today()
        if storage_is_configured():
            try:
                _pa_ensure = ensure_partitioned_storage()
                if _pa_ensure:
                    _pa_summary = get_data_summary()
                    if _pa_summary.get("total_rows", 0) > 0:
                        _pa_data_ok = True
                        _pa_min_d = date.fromisoformat(_pa_summary["min_date"])
                        _pa_max_d = date.fromisoformat(_pa_summary["max_date"])
            except Exception:
                pass

        if pa_view == "Daily":
            st.markdown("### Date")
            if _pa_data_ok:
                st.caption(f"{_pa_min_d} → {_pa_max_d}")
            _pa_date_default = _ss.get("pa_date", _pa_max_d)
            if isinstance(_pa_date_default, str):
                try:
                    _pa_date_default = date.fromisoformat(_pa_date_default)
                except Exception:
                    _pa_date_default = _pa_max_d
            if _pa_date_default < _pa_min_d or _pa_date_default > _pa_max_d:
                _pa_date_default = _pa_max_d
            pa_date = st.date_input(
                "Select date",
                value=_pa_date_default,
                min_value=_pa_min_d,
                max_value=_pa_max_d,
                label_visibility="collapsed",
                key="pa_date_picker",
            )
            _ss["pa_date"] = pa_date
            _pa_date_str = pa_date.isoformat()
        else:
            import calendar as _cal3
            st.markdown("### Month")
            _pa_avail_months = []
            _d = date(_pa_min_d.year, _pa_min_d.month, 1)
            _end_m = date(_pa_max_d.year, _pa_max_d.month, 1)
            while _d <= _end_m:
                _pa_avail_months.append((_d.year, _d.month))
                if _d.month == 12:
                    _d = date(_d.year + 1, 1, 1)
                else:
                    _d = date(_d.year, _d.month + 1, 1)
            if not _pa_avail_months:
                _pa_avail_months = [(_pa_max_d.year, _pa_max_d.month)]
            _pa_month_labels = [f"{_cal3.month_name[m]} {y}" for y, m in _pa_avail_months]
            _pa_sel_label = st.selectbox(
                "Select month", _pa_month_labels,
                index=len(_pa_avail_months) - 1,
                label_visibility="collapsed",
                key="pa_month_picker",
            )
            _pa_sel_idx = _pa_month_labels.index(_pa_sel_label)
            _pa_sel_year, _pa_sel_month = _pa_avail_months[_pa_sel_idx]
            _pa_date_str = f"{_pa_sel_year:04d}-{_pa_sel_month:02d}"
            _ss["pa_date"] = _pa_date_str

# ═════════════════════════════════════════════════════════════════════════════
# PENDING UPLOAD PROCESSING
# ═════════════════════════════════════════════════════════════════════════════
if _ss.get("pending_upload"):
    upload_list = _ss["pending_upload"]
    if isinstance(upload_list, dict):
        upload_list = [upload_list]

    _n_files = len(upload_list)
    _file_names = ", ".join(f['name'] for f in upload_list)
    render_header(map_type if "map_type" in dir() else "—",
                  "Processing upload…")

    with st.status(f"Processing {_n_files} file(s)…", expanded=True) as _upload_status:
        try:
            st.write(f"**Step 1 / 3** — Parsing {_n_files} file(s): {_file_names}")
            _parsed_frames = []
            for _uf_info in upload_list:
                _uf_df = parse_single_file(_uf_info["bytes"], filename=_uf_info["name"])
                st.write(
                    f"  • `{_uf_info['name']}`: **{len(_uf_df):,}** rows "
                    f"({_uf_df['complete_date'].min()} → {_uf_df['complete_date'].max()})"
                )
                _parsed_frames.append((_uf_info["name"], _uf_df))
            if len(_parsed_frames) == 1:
                new_df = _parsed_frames[0][1]
            else:
                new_df, _ = deduplicate_and_merge(_parsed_frames)
            st.write(f"Combined: **{len(new_df):,}** rows from {_n_files} file(s)")

            st.write("**Step 2 / 3** — Cleaning and validating…")
            _up_bad = int(
                new_df["Order Procedure"]
                .str.contains("\xa0", regex=False, na=False)
                .sum()
            )
            if _up_bad > 0:
                new_df = clean_procedure_names(new_df)
                st.write(f"Procedure names corrected ({_up_bad:,} row(s) fixed).")

            st.write("**Step 3 / 3** — Ingesting into partitioned storage…")
            stats = ingest_new_data(new_df)
            st.write(
                f"Storage updated: **{stats['rows_before']:,}** → "
                f"**{stats['rows_after']:,}** rows (+{stats['rows_added']:,} new)"
            )

            _ss.pop("pending_upload", None)
            st.cache_data.clear()
            _ss.pop("_partition_index", None)

            _upload_status.update(
                label=f"Done — added {stats['rows_added']:,} rows.",
                state="complete",
            )
            st.rerun()

        except Exception as _proc_err:
            _upload_status.update(
                label="Processing failed — existing data is unchanged.", state="error"
            )
            st.error(str(_proc_err))
            _ss.pop("pending_upload", None)

    st.stop()


# ═════════════════════════════════════════════════════════════════════════════
# MAIN PANEL — no data guard
# ═════════════════════════════════════════════════════════════════════════════
if _active_dashboard == "analytics" and not _data_exists:
    render_header(map_type if "map_type" in dir() else "Lab Productivity",
                  "—")
    st.info(
        "Welcome!  Upload a file or configure GitHub in your "
        "Streamlit secrets to start viewing lab productivity heatmaps."
    )
    st.stop()


# ═════════════════════════════════════════════════════════════════════════════
# MAIN PANEL — conditional on active dashboard
# ═════════════════════════════════════════════════════════════════════════════
if _active_dashboard == "analytics":
    if view_mode == "Daily":
        _is_forecast_view = selected_date > _max_d

        if _is_forecast_view:
            _fc_panel_data = load_forecasts(map_type)
            if _fc_panel_data is None:
                st.warning(
                    f"No forecast data available for **{map_type}**.  "
                    "Open Data Management and click **Refresh Forecast** to generate predictions."
                )
                st.stop()
            pivot, hours = build_forecast_pivot(
                _fc_panel_data, selected_date, hour_range, time_basis=time_basis
            )
            df_date_hour = None
            df_date      = pd.DataFrame()
        else:
            if storage_is_configured():
                filtered_df = load_filtered_data(
                    start_date=selected_date,
                    end_date=selected_date,
                    resources=_current_resources,
                    exclude_procs=_current_excludes,
                    _index_hash=_idx_hash,
                )
            else:
                from config import EXCLUDE_PROCS as _EP
                resources = _ss.resource_assignments[map_type]
                filtered_df = _local_df[
                    _local_df["Performing Service Resource"].isin(resources) &
                    ~_local_df["Order Procedure"].isin(_EP)
                ].copy()

            if time_basis == "In-Lab":
                _has_inlab = "inlab_date" in filtered_df.columns and filtered_df["inlab_date"].notna().any()
                if _has_inlab:
                    filtered_df = filtered_df[filtered_df["inlab_date"].notna()].copy()
                    filtered_df["complete_date"] = filtered_df["inlab_date"]
                    filtered_df["hour"] = filtered_df["inlab_hour"].astype(int)
                else:
                    st.warning("No 'Date/Time - In Lab' data available.")
                    st.stop()

            pivot, df_date_hour, df_date, hours = build_pivot(filtered_df, selected_date, hour_range)

        date_str = pd.Timestamp(selected_date).strftime("%B %d, %Y")
        _header_suffix = "  ·  Forecast" if _is_forecast_view else ""
        render_header(map_type, date_str + _header_suffix)

        if _is_forecast_view:
            st.markdown(
                '<div style="background:#1a1a1a;color:#e0e0e0;border-left:4px solid #FF9800;'
                'border-radius:6px;padding:0.85rem 1rem;font-size:0.82rem;margin-bottom:0.5rem;">'
                "This forecast is generated using Prophet, a forecasting ML model trained on all "
                "available historical data. It learns weekly patterns in procedure "
                "volume by hour of day. Predictions are based on limited training data and should "
                "be treated as estimates only."
                "</div>",
                unsafe_allow_html=True,
            )
            st.info(
                "Viewing **forecast** - these are predicted values, not actual completions. "
                "Use the date picker or ◄ Prev / Next ► to return to historical dates."
            )

        if pivot is None:
            if _is_forecast_view:
                st.warning(
                    f"No forecast predictions available for **{map_type}** on **{date_str}** "
                    f"within the selected hour range."
                )
            else:
                st.warning(
                    f"No data found for **{map_type}** on **{date_str}** "
                    f"within the selected hour range.  Try widening the hour slider."
                )
            st.stop()

        _hour_cols  = [c for c in pivot.columns if c != "Total"]
        if not _hour_cols or pivot.empty:
            st.info("No completed procedures found for this site on the selected date.")
            st.stop()
        total_vol   = int(round(pivot["Total"].sum()))
        top_proc    = pivot["Total"].idxmax()
        peak_hour   = pivot[_hour_cols].sum().idxmax()
        num_procs   = len(pivot)
        avg_per_hr  = round(total_vol / max(len(_hour_cols), 1), 1)

        _vol_label  = "Forecast Volume" if _is_forecast_view else "Total Volume"

        _m1, _m2, _m3, _m4, _m5 = st.columns(5)
        with _m1:
            st.markdown(metric_card(_vol_label, f"{total_vol:,}", accent=True),
                        unsafe_allow_html=True)
        with _m2:
            _tp_disp = top_proc[:28] + "…" if len(top_proc) > 28 else top_proc
            st.markdown(metric_card("Top Procedure", _tp_disp,
                        sub=f"{int(round(pivot.loc[top_proc, 'Total'])):,} total"),
                        unsafe_allow_html=True)
        with _m3:
            st.markdown(metric_card("Peak Hour", peak_hour,
                        sub=f"{int(round(pivot[_hour_cols].sum()[peak_hour]))} "
                            f"{'predicted' if _is_forecast_view else 'completions'}"),
                        unsafe_allow_html=True)
        with _m4:
            st.markdown(metric_card("Procedures", str(num_procs), sub="shown (top 30)"),
                        unsafe_allow_html=True)
        with _m5:
            st.markdown(metric_card("Avg / Hour", str(avg_per_hr),
                        sub=f"across {len(_hour_cols)} hours"),
                        unsafe_allow_html=True)

        st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

        if _is_forecast_view:
            _heading_label = "Forecast Volume by Procedure &amp; Hour"
        elif time_basis == "In-Lab":
            _heading_label = "In-Lab Volume by Procedure &amp; Hour"
        else:
            _heading_label = "Completed Volume by Procedure &amp; Hour"
        st.markdown(
            f'<div class="section-heading">{_heading_label}</div>',
            unsafe_allow_html=True,
        )

        if _is_forecast_view:
            st.markdown(
                f'<div class="heatmap-legend">'
                f'Colour scale: &nbsp;<strong style="color:#FFE0B2;">■</strong> low &nbsp;→&nbsp; '
                f'<strong style="color:#E65100;">■</strong> high (≥ {VMAX[map_type]} / hour). &nbsp;'
                f'<strong>Total</strong> column = predicted daily sum per procedure.'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="heatmap-legend">'
                f'Colour scale: &nbsp;<strong style="color:#f5e642;">■</strong> low &nbsp;→&nbsp; '
                f'<strong style="color:#3b0f70;">■</strong> high (≥ {VMAX[map_type]} / hour). &nbsp;'
                f'<strong>Total</strong> column = full-day sum per procedure.'
                f'</div>',
                unsafe_allow_html=True,
            )

        _table_h = min(80 + 35 * len(pivot), 900)
        if _is_forecast_view:
            st.dataframe(style_forecast_pivot(pivot, VMAX[map_type]),
                         width="stretch", height=_table_h)
        else:
            st.dataframe(style_pivot(pivot, VMAX[map_type]),
                         width="stretch", height=_table_h)

        _file_prefix = map_type.replace(" ", "_")
        _date_tag    = pd.Timestamp(selected_date).strftime("%Y-%m-%d")

        if not _is_forecast_view and df_date_hour is not None and not df_date_hour.empty:
            _dl1, _dl2 = st.columns(2)
            with _dl1:
                st.download_button(
                    "Download PNG",
                    data=build_png(df_date_hour, map_type, selected_date, hours),
                    file_name=f"{_file_prefix}_{_date_tag}.png",
                    mime="image/png",
                    width="stretch",
                    key="daily_png_dl",
                )
            with _dl2:
                _cols_for_csv = [c for c in ["Order Procedure", "Performing Service Resource",
                                              "Date/Time - Complete", "Complete Volume"]
                                if c in df_date.columns]
                _daily_raw_csv = (
                    df_date[_cols_for_csv]
                    .sort_values("Date/Time - Complete")
                    .reset_index(drop=True)
                    .to_csv(index=False)
                    .encode()
                )
                st.download_button(
                    "Download CSV",
                    data=_daily_raw_csv,
                    file_name=f"{_file_prefix}_raw_{_date_tag}.csv",
                    mime="text/csv",
                    width="stretch",
                    key="daily_csv_dl",
                )

        st.markdown("---")

        with st.expander("Hourly Volume", expanded=False):
            _hourly = pivot[_hour_cols].sum().reset_index()
            _hourly.columns = ["Hour", "Total Volume"]
            _hourly_chart = (
                alt.Chart(_hourly)
                .mark_bar()
                .encode(
                    x=alt.X("Hour:N", sort=list(_hour_cols), title="Hour"),
                    y=alt.Y("Total Volume:Q", title="Total Volume"),
                )
                .properties(height=220)
            )
            st.altair_chart(_hourly_chart, use_container_width=True)

        if not _is_forecast_view:
            with st.expander("Drill into a cell — individual completion events", expanded=False):
                st.markdown(
                    "Select a **procedure** and **hour** to inspect every individual "
                    "completion event recorded in that cell."
                )
                _dd1, _dd2 = st.columns(2)
                with _dd1:
                    sel_proc       = st.selectbox("Procedure", pivot.index.tolist(), key="drill_proc")
                with _dd2:
                    sel_hour_label = st.selectbox("Hour", _hour_cols, key="drill_hour")

                sel_hour_int = LABEL_TO_HOUR[sel_hour_label]
                detail       = df_date[
                    (df_date["Order Procedure"] == sel_proc) &
                    (df_date["hour"] == sel_hour_int)
                ].copy().sort_values("Date/Time - Complete")

                _show_cols = {k: v for k, v in {
                    "Date/Time - Complete":        "Completed At",
                    "Performing Service Resource": "Resource",
                    "Complete Volume":             "Volume",
                }.items() if k in detail.columns}

                detail_display = (
                    detail[list(_show_cols.keys())]
                    .rename(columns=_show_cols)
                    .reset_index(drop=True)
                )
                if "Completed At" in detail_display.columns:
                    detail_display["Completed At"] = (
                        pd.to_datetime(detail_display["Completed At"])
                        .dt.strftime("%Y-%m-%d  %H:%M:%S")
                    )

                if detail_display.empty:
                    st.info(
                        f"No completions for **{sel_proc}** during **{sel_hour_label}** on {date_str}."
                    )
                else:
                    _cell_vol = (
                        int(detail_display["Volume"].sum())
                        if "Volume" in detail_display.columns else len(detail_display)
                    )
                    st.markdown(
                        f"**{len(detail_display)} event(s)** &nbsp;·&nbsp; *{sel_proc}* &nbsp;·&nbsp; "
                        f"**{sel_hour_label}** &nbsp;·&nbsp; Total volume: **{_cell_vol}**"
                    )
                    st.dataframe(
                        detail_display, width="stretch",
                        height=min(80 + 35 * len(detail_display), 500),
                    )

    else:
        month_name_str = f"{_cal.month_name[selected_month]} {selected_year}"
        render_header(map_type, month_name_str)

        _month_start = date(selected_year, selected_month, 1)
        _month_end = date(selected_year, selected_month,
                          _cal.monthrange(selected_year, selected_month)[1])

        if storage_is_configured():
            filtered_df = load_filtered_data(
                start_date=_month_start,
                end_date=_month_end,
                resources=_current_resources,
                exclude_procs=_current_excludes,
                _index_hash=_idx_hash,
            )
        else:
            resources = _ss.resource_assignments[map_type]
            filtered_df = _local_df[
                _local_df["Performing Service Resource"].isin(resources) &
                ~_local_df["Order Procedure"].isin(EXCLUDE_PROCS)
            ].copy()

        if time_basis == "In-Lab":
            _has_inlab = "inlab_date" in filtered_df.columns and filtered_df["inlab_date"].notna().any()
            if _has_inlab:
                filtered_df = filtered_df[filtered_df["inlab_date"].notna()].copy()
                filtered_df["complete_date"] = filtered_df["inlab_date"]
                filtered_df["hour"] = filtered_df["inlab_hour"].astype(int)
            else:
                st.warning("No 'Date/Time - In Lab' data available.")
                st.stop()

        monthly_pivot, n_days, month_raw_df = build_monthly_pivot(
            filtered_df, selected_year, selected_month
        )

        if monthly_pivot is None:
            st.warning(f"No data found for **{map_type}** in **{month_name_str}**.")
            st.stop()

        _m_hour_cols  = [c for c in monthly_pivot.columns if c != "Total"]
        _m_total_vol  = int(round(monthly_pivot["Total"].sum() * n_days))
        _m_top_proc   = monthly_pivot["Total"].idxmax()
        _m_peak_col   = monthly_pivot[_m_hour_cols].sum().idxmax()
        _m_peak_disp  = _m_peak_col.replace("AM", " AM").replace("PM", " PM")
        _m_n_procs    = len(monthly_pivot)
        _m_avg_per_day = round(_m_total_vol / max(n_days, 1))

        _mm1, _mm2, _mm3, _mm4, _mm5 = st.columns(5)
        with _mm1:
            st.markdown(metric_card("Total Volume", f"{_m_total_vol:,}", accent=True),
                        unsafe_allow_html=True)
        with _mm2:
            _mtp_disp = _m_top_proc[:28] + "…" if len(_m_top_proc) > 28 else _m_top_proc
            st.markdown(metric_card("Top Procedure", _mtp_disp,
                        sub=f"highest volume in {month_name_str}"),
                        unsafe_allow_html=True)
        with _mm3:
            st.markdown(metric_card("Peak Hour", _m_peak_disp,
                        sub="highest avg volume"), unsafe_allow_html=True)
        with _mm4:
            st.markdown(metric_card("Procedures Shown", str(_m_n_procs),
                        sub="top 30 by month volume"), unsafe_allow_html=True)
        with _mm5:
            st.markdown(metric_card("Avg / Day", f"{_m_avg_per_day:,}",
                        sub=f"over {n_days} days"), unsafe_allow_html=True)

        st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

        st.markdown(
            f'<div class="section-heading">'
            f'{map_type} - Monthly Average | {month_name_str} | N = {n_days} days'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="heatmap-legend">'
            f'Values = avg completed volume per day in hour. '
            f'Colour scale: &nbsp;<strong style="color:#f5e642;">■</strong> low &nbsp;→&nbsp; '
            f'<strong style="color:#3b0f70;">■</strong> high (≥ {VMAX[map_type]}). &nbsp;'
            f'<strong>Total</strong> column = avg daily total per procedure.'
            f'</div>',
            unsafe_allow_html=True,
        )

        _m_table_h = min(80 + 35 * len(monthly_pivot), 900)
        st.dataframe(
            style_monthly_pivot(monthly_pivot, VMAX[map_type]),
            width="stretch", height=_m_table_h,
        )

        _m_file_prefix = map_type.replace(" ", "_")
        _m_month_label = _cal.month_name[selected_month]
        _m_file_tag    = f"{_m_month_label}_{selected_year}"
        _mdl1, _mdl2   = st.columns(2)
        with _mdl1:
            st.download_button(
                "Download PNG",
                data=build_monthly_png(
                    monthly_pivot, map_type, selected_year, selected_month, n_days
                ),
                file_name=f"{_m_file_prefix}_{_m_file_tag}.png",
                mime="image/png",
                width="stretch",
                key="monthly_png_dl",
            )
        with _mdl2:
            _csv_cols = [c for c in ["Order Procedure", "Performing Service Resource",
                                      "Date/Time - Complete", "Complete Volume"]
                         if c in filtered_df.columns]
            _monthly_raw_csv = (
                filtered_df[_csv_cols]
                .sort_values("Date/Time - Complete")
                .reset_index(drop=True)
                .to_csv(index=False)
                .encode()
            )
            st.download_button(
                "Download CSV",
                data=_monthly_raw_csv,
                file_name=f"{_m_file_prefix}_raw_{_m_file_tag}.csv",
                mime="text/csv",
                width="stretch",
                key="monthly_csv_dl",
            )

        st.markdown("---")

        with st.expander("Hourly Volume by Day of Week", expanded=False):
            weekday_pivot, _wd_counts = build_weekday_pivot(
                month_raw_df, selected_year, selected_month
            )

            if weekday_pivot is None:
                st.info("No data available for weekday breakdown.")
            else:
                _wd_hour_cols    = [c for c in weekday_pivot.columns if c != "Total"]
                _wd_busiest_day  = weekday_pivot["Total"].idxmax()
                _wd_lightest_day = weekday_pivot["Total"].idxmin()
                _wd_peak_hour    = weekday_pivot[_wd_hour_cols].sum().idxmax()
                _wd_peak_disp    = _wd_peak_hour.replace("AM", " AM").replace("PM", " PM")

                _wc1, _wc2, _wc3 = st.columns(3)
                with _wc1:
                    st.markdown(
                        metric_card(
                            "Busiest Day",
                            _wd_busiest_day.split("  ")[0],
                            sub=f"avg {int(round(weekday_pivot.loc[_wd_busiest_day, 'Total']))} vol / day",
                        ),
                        unsafe_allow_html=True,
                    )
                with _wc2:
                    st.markdown(
                        metric_card("Peak Hour", _wd_peak_disp,
                                    sub="highest avg volume across weekdays"),
                        unsafe_allow_html=True,
                    )
                with _wc3:
                    st.markdown(
                        metric_card(
                            "Lightest Day",
                            _wd_lightest_day.split("  ")[0],
                            sub=f"avg {int(round(weekday_pivot.loc[_wd_lightest_day, 'Total']))} vol / day",
                        ),
                        unsafe_allow_html=True,
                    )

                st.markdown(
                    f'<div class="section-heading">'
                    f'{map_type} - Weekday Pattern | {month_name_str}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                _wd_vmax = max(1, int(weekday_pivot[_wd_hour_cols].values.max()))
                st.markdown(
                    f'<div class="heatmap-legend">'
                    f'Values = avg completed volume per occurrence of that weekday. '
                    f'Colour scale: &nbsp;<strong style="color:#f5e642;">■</strong> low &nbsp;→&nbsp; '
                    f'<strong style="color:#3b0f70;">■</strong> high (≥ {_wd_vmax}). &nbsp;'
                    f'<strong>Total</strong> column = avg daily total for that weekday.'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                st.dataframe(
                    style_monthly_pivot(weekday_pivot, _wd_vmax),
                    width="stretch",
                    height=min(80 + 35 * 7, 400),
                )

                _wdl1, _wdl2 = st.columns(2)
                with _wdl1:
                    st.download_button(
                        "Download PNG",
                        data=build_weekday_png(
                            weekday_pivot, map_type, selected_year, selected_month
                        ),
                        file_name=f"{_m_file_prefix}_Weekday_{_m_file_tag}.png",
                        mime="image/png",
                        width="stretch",
                        key="weekday_png_dl",
                    )
                with _wdl2:
                    st.download_button(
                        "Download CSV",
                        data=weekday_pivot.to_csv(index=True).encode(),
                        file_name=f"{_m_file_prefix}_Weekday_{_m_file_tag}.csv",
                        mime="text/csv",
                        width="stretch",
                        key="weekday_csv_dl",
                    )

else:
    # ═══════════════════════════════════════════════════════════════════════
    # PRE-ANALYTICS MAIN PANEL
    # ═══════════════════════════════════════════════════════════════════════
    st.write("Pre-analytics block reached")
    try:
        import plotly.graph_objects as _pgo
        import numpy as _np

        pa_location = _ss.get("pa_location_radio", "Keck")
        pa_view     = _ss.get("pa_view_radio", "Daily")
        _pa_ds      = _ss.get("pa_date", date.today().isoformat())
        if isinstance(_pa_ds, date):
            _pa_ds = _pa_ds.isoformat()

        if len(_pa_ds) == 7:
            import calendar as _calpa
            _pa_yr, _pa_mo = int(_pa_ds[:4]), int(_pa_ds[5:7])
            _pa_date_label = f"{_calpa.month_name[_pa_mo]} {_pa_yr}"
        else:
            _pa_date_label = pd.Timestamp(_pa_ds).strftime("%B %d, %Y")

        render_header(f"Pre-Analytics · {pa_location}", _pa_date_label)

        _draw_df, _draw_debug = load_draw_data(_pa_ds, pa_view)
        _staff_dict, _staff_raw = load_phlebotomy_staff()

        with st.expander("Debug — name matching", expanded=True):
            st.write("CSV Drawn Tech — first 5 raw values:", _staff_raw)
            st.write("Staff lookup keys — first 10 (normalized):", _draw_debug.get("staff_keys", []))
            st.write("Parquet Drawn Tech — first 10 raw values:", _draw_debug.get("raw_drawn_tech", []))
            st.write("Rows before name-match filter:", _draw_debug.get("rows_before", 0))
            st.write("Rows after name-match filter:", _draw_debug.get("rows_after", 0))

        _loc_df = _draw_df[_draw_df["location"] == pa_location] if not _draw_df.empty else _draw_df
        _pa_total_draws   = len(_loc_df)
        _pa_total_samples = int(_loc_df["samples"].sum()) if not _loc_df.empty else 0
        _pa_active_techs  = int(_loc_df["display_name"].nunique()) if not _loc_df.empty else 0
        if not _loc_df.empty:
            _peak_h_val   = int(_loc_df.groupby("hour").size().idxmax())
            _pa_peak_hour = HOUR_LABELS.get(_peak_h_val, str(_peak_h_val))
        else:
            _pa_peak_hour = "—"

        _kc1, _kc2, _kc3, _kc4 = st.columns(4)
        with _kc1:
            st.markdown(metric_card("Total Draws", f"{_pa_total_draws:,}", accent=True),
                        unsafe_allow_html=True)
        with _kc2:
            st.markdown(metric_card("Total Samples", f"{_pa_total_samples:,}"),
                        unsafe_allow_html=True)
        with _kc3:
            st.markdown(metric_card("Active Techs", str(_pa_active_techs)),
                        unsafe_allow_html=True)
        with _kc4:
            st.markdown(metric_card("Peak Hour", _pa_peak_hour), unsafe_allow_html=True)

        st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

        _PA_HOUR_LABELS = [HOUR_LABELS[h] for h in range(24)]

        _PA_SHIFT_ORDER = {
            "Keck":   ["Early AM", "AM", "PM", "NS"],
            "Norris": ["AM", "PM", "NS"],
            "HC3":    [None],
        }

        def _render_pa_heatmap(draw_df, location, shift, view, heatmap_key):
            _pivot = build_draw_pivot(draw_df, location, shift, view)
            _techs = _pivot.index.tolist()
            _z     = _pivot.values.tolist()
            _x     = _PA_HOUR_LABELS

            _flat  = [v for row in _z for v in row if v > 0]
            _vmax_pa = float(_np.percentile(_flat, 95)) if _flat else 1.0
            _vmax_pa = max(_vmax_pa, 1.0)

            _text_vals = [
                [str(int(round(v))) if v > 0 else "" for v in row]
                for row in _z
            ]

            _fig = _pgo.Figure(data=_pgo.Heatmap(
                z=_z,
                x=_x,
                y=_techs,
                text=_text_vals,
                texttemplate="%{text}",
                colorscale="Teal",
                zmin=0,
                zmax=_vmax_pa,
                xgap=1,
                ygap=1,
                colorbar=dict(title="Draws/hour", thickness=12, len=0.9),
            ))
            _plot_h = max(250, len(_techs) * 35 + 80)
            _fig.update_layout(
                height=_plot_h,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                xaxis=dict(tickfont=dict(size=10), side="bottom"),
                yaxis=dict(tickfont=dict(size=11), autorange="reversed"),
            )

            _sel_event = st.plotly_chart(
                _fig,
                use_container_width=True,
                on_select="rerun",
                key=heatmap_key,
            )

            _cell_key = f"selected_cell_{heatmap_key}"
            if _sel_event and hasattr(_sel_event, "selection") and _sel_event.selection:
                _pts = _sel_event.selection.get("points", [])
                if _pts:
                    _pt = _pts[0]
                    _sel_tech   = _pt.get("y")
                    _sel_hlabel = _pt.get("x")
                    _sel_hour   = next(
                        (h for h, lbl in HOUR_LABELS.items() if lbl == _sel_hlabel), None
                    )
                    if _sel_tech is not None and _sel_hour is not None:
                        _ss[_cell_key] = {
                            "tech": _sel_tech, "hour": _sel_hour, "hlabel": _sel_hlabel
                        }

            _stored = _ss.get(_cell_key)
            if _stored:
                _d_tech   = _stored["tech"]
                _d_hour   = _stored["hour"]
                _d_hlabel = _stored["hlabel"]
                _detail   = (
                    draw_df[
                        (draw_df["display_name"] == _d_tech) &
                        (draw_df["hour"] == _d_hour)
                    ].copy()
                    if not draw_df.empty else pd.DataFrame()
                )
                st.markdown(
                    f'<div class="section-heading">Draws for {_d_tech} at {_d_hlabel}</div>',
                    unsafe_allow_html=True,
                )
                if _detail.empty:
                    st.info(f"No draws for **{_d_tech}** at **{_d_hlabel}**.")
                else:
                    _detail_disp = _detail[["draw_datetime", "samples"]].copy()
                    _detail_disp["draw_datetime"] = pd.to_datetime(
                        _detail_disp["draw_datetime"]
                    ).dt.strftime("%Y-%m-%d %H:%M")
                    _detail_disp.columns = ["Draw Timestamp", "Samples"]
                    _detail_disp = _detail_disp.sort_values("Draw Timestamp").reset_index(drop=True)
                    st.dataframe(
                        _detail_disp, width="stretch",
                        height=min(80 + 35 * len(_detail_disp), 450),
                    )
                    st.caption(
                        f"{len(_detail_disp)} draw(s)  ·  "
                        f"{int(_detail_disp['Samples'].sum())} total sample(s)"
                    )

        for _pa_shift in _PA_SHIFT_ORDER.get(pa_location, [None]):
            if pa_location != "HC3" and _pa_shift is not None:
                st.subheader(f"{pa_location} — {_pa_shift}")
            _hkey = f"heatmap_{pa_location}_{_pa_shift or 'all'}"
            _render_pa_heatmap(_draw_df, pa_location, _pa_shift, pa_view, _hkey)
    except Exception as e:
        st.exception(e)
