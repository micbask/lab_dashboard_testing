"""
analytics/dashboard.py — Analytics page controller.

Renders the sidebar (bench / time basis / view / date / hour-range
pickers + Data Management), processes pending file uploads, and
dispatches to one of three view modules:

  • analytics.views.tat     — TAT (turnaround time) view
  • analytics.views.daily   — Daily heatmap + Hourly volume bar
  • analytics.views.monthly — Monthly heatmap + day-of-week breakdown

Shared helpers (heatmap builder, top-N legend, local-file scope,
TAT colors, top-N options) live in analytics.views._shared. The
file was split in Batch 5 Phase 2 — see git log for the per-view
extractions.
"""

import calendar as _cal
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from config import (
    DEFAULT_RESOURCES, MAP_TYPES,
    BENCH_LABEL_TO_VALUE,
)
from storage import (
    storage_is_configured, get_data_summary,
    delete_date_range, reset_all_data,
    ensure_partitioned_storage, get_index_hash,
    count_rows_in_date_range,
)
from forecasting import (
    load_forecasts, retrain_all_forecasts_streaming,
)
from parsing import parse_single_file, deduplicate_and_merge, clean_procedure_names
from ui_components import (
    render_header, render_data_management_sidebar,
)
from analytics.views._shared import VALID_TOP_N
from analytics.views.tat import render_tat_view
from analytics.views.daily import render_daily_view
from analytics.views.monthly import render_monthly_view


# ════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ════════════════════════════════════════════════════════════════════════════

def render_sidebar(ss) -> dict:
    """Render analytics sidebar widgets. Returns params dict for render()."""
    with st.sidebar:
        # ── 1. Testing Bench (same selector for every Time Basis,
        #      including TAT — TAT now reports turnaround for the
        #      performing bench rather than the patient's facility).
        if "time_basis" not in st.session_state:
            st.session_state["time_basis"] = "Completed"

        st.markdown(
            '<div class="sidebar-section-label">TESTING BENCH</div>',
            unsafe_allow_html=True,
        )
        _bench_short = st.radio(
            "Testing Bench",
            list(BENCH_LABEL_TO_VALUE.keys()),
            horizontal=True, label_visibility="collapsed",
            key="analytics_bench_short",
        )
        map_type = BENCH_LABEL_TO_VALUE[_bench_short]

        if ss.last_map_type != map_type:
            ss.pop("date_picker", None)
            ss.last_map_type = map_type

        # ── 2. Time Basis  (keyed; widget owns session_state) ──
        st.markdown(
            '<div class="sidebar-section-label">TIME BASIS</div>',
            unsafe_allow_html=True,
        )
        time_basis = st.radio(
            "Time Basis", ["Completed", "In-Lab", "TAT"],
            horizontal=True, label_visibility="collapsed",
            key="time_basis",
        )

        # Switching time_basis can leave the user on a date that has
        # data for the previous basis but is empty for the new one
        # (e.g. Completed has May 15, In-Lab doesn't). Pop the date
        # picker so it re-clamps to the bench's _max_d on the next
        # render, the same invalidation pattern as a bench change.
        if ss.get("last_time_basis") != time_basis:
            ss.pop("date_picker", None)
            ss["last_time_basis"] = time_basis

        # ── 3. View ──
        st.markdown(
            '<div class="sidebar-section-label">VIEW</div>',
            unsafe_allow_html=True,
        )
        view_mode = st.radio(
            "View", ["Daily", "Monthly"],
            horizontal=True, label_visibility="collapsed",
        )

        # ── Pending background tasks (kicked off by buttons on a previous run) ──
        if ss.pop("pending_forecast_retrain", False):
            if storage_is_configured():
                with st.spinner("Retraining forecast models…"):
                    retrain_all_forecasts_streaming(ss.resource_assignments)
                st.success("Forecast models retrained.")
            else:
                st.warning("No storage configured - cannot retrain forecasts.")

        if ss.pop("pending_reset", False):
            try:
                if storage_is_configured():
                    reset_all_data()
                    st.cache_data.clear()
                    st.success("Master dataset cleared.")
                # Forecasts are now in @st.cache_data (forecasting.load_forecasts),
                # which the global cache_data.clear() above already invalidated.
                # The historic per-map session_state pop is no longer needed.
            except Exception as _rst_err:
                st.error(f"Reset failed: {_rst_err}")

        if "pending_delete_range" in ss:
            del_info = ss.pop("pending_delete_range")
            try:
                result = delete_date_range(del_info["start"], del_info["end"])
                st.cache_data.clear()
                st.success(
                    f"Deleted {result['rows_removed']:,} rows "
                    f"({del_info['start']} → {del_info['end']})."
                )
            except Exception as _dr_err:
                st.error(f"Delete failed: {_dr_err}")

        # ── Silent data-existence detection ──
        # Fast partition-index read with NO UI. The Data Source caption
        # / status chip / Data Management expander render later at the
        # bottom of the sidebar; we just need _data_exists now so the
        # Date and Hour Range sections above the Data Source block know
        # whether they have anything to show.
        _data_exists = False
        _data_summary = {"total_rows": 0, "partitions": 0}
        _local_df = pd.DataFrame()
        _load_err = None

        if storage_is_configured():
            try:
                _data_exists = ensure_partitioned_storage()
                if _data_exists:
                    _data_summary = get_data_summary()
                    if _data_summary["total_rows"] == 0:
                        _data_exists = False
            except Exception as _e:
                _load_err = _e
                _data_exists = False

        # Defaults — overwritten when _data_exists.
        _min_d = date.today()
        _max_d = date.today()
        _current_resources = ()
        _idx_hash = ""
        selected_date = date.today()
        selected_year = date.today().year
        selected_month = date.today().month
        hour_range = (0, 23)
        _fc_data = None
        _is_forecast_date = False

        if _data_exists:
            # All three views (Completed / In-Lab / TAT) now share the
            # same data scope: bench-level resources + EXCLUDED_PROCEDURES.
            # The exclusion is applied inside `load_analytics_data`; the
            # sidebar only needs to supply the bench resources here.
            _current_resources = tuple(sorted(ss.resource_assignments[map_type]))
            _idx_hash = get_index_hash() if storage_is_configured() else ""

            if storage_is_configured():
                _min_d = date.fromisoformat(_data_summary["min_date"])
                _max_d = date.fromisoformat(_data_summary["max_date"])

            # ── 4. Date  (Daily date picker + Prev/Next OR Monthly selector) ──
            if view_mode == "Daily":
                if time_basis == "TAT":
                    _fc_data = None
                    _fc_max_d = _max_d
                else:
                    _fc_data = load_forecasts(map_type)
                    _fc_max_d = _max_d
                    if _fc_data:
                        _fc_end_d   = _fc_data["forecast_end"]
                        _fc_max_d = _fc_end_d

                # Prev/Next clicks write `_pending_date` and rerun;
                # this block pulls the pending date into session_state
                # *before* the date_input renders, so st.date_input
                # picks it up as the new value on this render.
                if "_pending_date" in ss:
                    _pending = ss.pop("_pending_date")
                    if _min_d <= _pending <= _fc_max_d:
                        ss["date_picker"] = _pending

                if (
                    "date_picker" not in ss
                    or ss["date_picker"] < _min_d
                    or ss["date_picker"] > _fc_max_d
                ):
                    ss["date_picker"] = _max_d

                st.markdown(
                    '<div class="sidebar-section-label">DATE</div>',
                    unsafe_allow_html=True,
                )

                picked_date = st.date_input(
                    "Select date",
                    min_value=_min_d,
                    max_value=_fc_max_d,
                    label_visibility="collapsed",
                    key="date_picker",
                )
                selected_date = picked_date
                _is_forecast_date = selected_date > _max_d

                st.markdown(
                    f'<div class="sidebar-meta-caption">'
                    f'{_min_d} → {_fc_max_d}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                # Prev / Next nav buttons.
                _nc1, _nc2 = st.columns([1, 1], gap="medium")
                with _nc1:
                    if st.button(
                        "←",
                        disabled=(selected_date <= _min_d),
                        key="nav_prev_date",
                        width="stretch",
                    ):
                        ss["_pending_date"] = selected_date - timedelta(days=1)
                        st.rerun()
                with _nc2:
                    if st.button(
                        "→",
                        disabled=(selected_date >= _fc_max_d),
                        key="nav_next_date",
                        width="stretch",
                    ):
                        ss["_pending_date"] = selected_date + timedelta(days=1)
                        st.rerun()

                if time_basis != "TAT":
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
                        st.caption("No forecast available - use Refresh Forecast to generate one.")

                    # ── 5. Hour Range  (Completed / In-Lab only) ──
                    st.markdown(
                        '<div class="sidebar-section-label">HOUR RANGE</div>',
                        unsafe_allow_html=True,
                    )

                    def _fmt_h(h: int) -> str:
                        hr12 = 12 if h % 12 == 0 else h % 12
                        suf  = "AM" if h < 12 else "PM"
                        return f"{hr12}:00 {suf}"

                    hour_range = st.slider(
                        "Hours", 0, 23, (0, 23), label_visibility="collapsed"
                    )
                    st.markdown(
                        f'<div class="sidebar-meta-caption">'
                        f'{_fmt_h(hour_range[0])} → {_fmt_h(hour_range[1])}'
                        f'</div>',
                        unsafe_allow_html=True,
                    )

            else:  # Monthly
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

                st.markdown(
                    '<div class="sidebar-section-label">MONTH</div>',
                    unsafe_allow_html=True,
                )
                _sel_month_label = st.selectbox(
                    "Select month",
                    _month_labels,
                    index=len(_avail_months) - 1,
                    label_visibility="collapsed",
                )
                _sel_idx = _month_labels.index(_sel_month_label)
                selected_year, selected_month = _avail_months[_sel_idx]

        # ── 6. Data Management (shared component) ───────────────────
        if storage_is_configured():
            render_data_management_sidebar(
                ss,
                data_exists=_data_exists,
                data_summary=_data_summary,
                load_err=_load_err,
            )
        else:
            st.markdown("**Data source:** Local file upload")
            st.caption(
                "Configure GitHub in Streamlit secrets to enable persistent storage."
            )
            uploaded_files = st.file_uploader(
                "Upload Excel file(s)", type=["xlsx", "xls", "csv"],
                accept_multiple_files=True,
            )
            if uploaded_files:
                _parsed = []
                for f in uploaded_files:
                    _df = parse_single_file(f.read(), filename=f.name)
                    if not _df.empty:
                        _parsed.append((f.name, _df))
                if _parsed:
                    _local_df, _ = deduplicate_and_merge(_parsed)
                    _data_exists = not _local_df.empty

    # TAT view needs a single canonical date string the renderer can
    # convert to (start_date, end_date) for `load_analytics_data` —
    # 'YYYY-MM-DD' for Daily, 'YYYY-MM' for Monthly. Empty string when
    # not on the TAT path.
    _tat_date_str = ""
    if time_basis == "TAT" and _data_exists:
        if view_mode == "Daily":
            _tat_date_str = selected_date.isoformat()
        else:
            _tat_date_str = f"{selected_year:04d}-{selected_month:02d}"

    return {
        "map_type": map_type,
        "view_mode": view_mode,
        "time_basis": time_basis,
        "data_exists": _data_exists,
        "local_df": _local_df,
        "current_resources": _current_resources,
        "idx_hash": _idx_hash,
        "selected_date": selected_date,
        "selected_year": selected_year,
        "selected_month": selected_month,
        "hour_range": hour_range,
        "is_forecast_date": _is_forecast_date,
        # TAT-only fields; populated when time_basis == "TAT". The
        # `tat_bench` value is the full bench name ("Keck Core",
        # "Norris Core", "Norris Specialty") — same source as map_type.
        "tat_bench": map_type if time_basis == "TAT" else None,
        "tat_date_str": _tat_date_str,
    }


# ════════════════════════════════════════════════════════════════════════════
# UPLOAD HANDLER
# ════════════════════════════════════════════════════════════════════════════

def _handle_pending_upload(map_type: str, upload_list, ss) -> None:
    """Process a pending file upload: parse, dedup, clean, ingest, rerun."""
    if isinstance(upload_list, dict):
        upload_list = [upload_list]

    _n_files   = len(upload_list)
    _file_names = ", ".join(f['name'] for f in upload_list)
    render_header(map_type, "Processing upload…")

    with st.status(f"Processing {_n_files} file(s)…", expanded=True) as _upload_status:
        try:
            st.write(f"**Step 1 / 3** - Parsing {_n_files} file(s): {_file_names}")
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

            st.write("**Step 2 / 3** - Cleaning and validating…")
            _up_bad = int(
                new_df["Order Procedure"]
                .str.contains("\xa0", regex=False, na=False)
                .sum()
            )
            if _up_bad > 0:
                new_df = clean_procedure_names(new_df)
                st.write(f"Procedure names corrected ({_up_bad:,} row(s) fixed).")

            st.write("**Step 3 / 3** - Ingesting into partitioned storage…")
            from storage import ingest_new_data
            stats = ingest_new_data(new_df)
            st.write(
                f"Storage updated: **{stats['rows_before']:,}** → "
                f"**{stats['rows_after']:,}** rows (+{stats['rows_added']:,} new)"
            )

            ss.pop("pending_upload", None)
            # Drop the cached partition index so the next get_index_hash()
            # call recomputes. The new hash flows through every
            # @st.cache_data fn that includes idx_hash in its key —
            # which is every data-loading path — so they re-fetch on
            # the next access.
            ss.pop("_partition_index", None)
            _upload_status.update(
                label=f"Done - added {stats['rows_added']:,} rows.",
                state="complete",
            )
            st.rerun()

        except Exception as _proc_err:
            _upload_status.update(
                label="Processing failed - existing data is unchanged.", state="error"
            )
            st.error(str(_proc_err))
            ss.pop("pending_upload", None)


# ════════════════════════════════════════════════════════════════════════════
# MAIN DISPATCHER
# ════════════════════════════════════════════════════════════════════════════

def render(params: dict, ss) -> None:
    """Render the analytics main panel.

    Dispatches to the upload handler, the no-data placeholder, the TAT
    view, the Daily view, or the Monthly view. All early exits use
    `return` rather than `st.stop()` so app.py's footer line is always
    reached on every dashboard view.
    """
    map_type     = params["map_type"]
    view_mode    = params["view_mode"]
    time_basis   = params["time_basis"]
    _data_exists = params["data_exists"]

    if ss.get("pending_upload"):
        _handle_pending_upload(map_type, ss["pending_upload"], ss)
        return

    if not _data_exists:
        render_header(map_type, "-")
        st.info(
            "Welcome!  Upload a file or configure GitHub in your "
            "Streamlit secrets to start viewing lab productivity heatmaps."
        )
        return

    if time_basis == "TAT":
        render_tat_view(params)
        return

    # Top-N state init (shared by Daily + Monthly volume views).
    if (
        "analytics_top_n" not in st.session_state
        or st.session_state["analytics_top_n"] not in VALID_TOP_N
    ):
        st.session_state["analytics_top_n"] = 10

    if view_mode == "Daily":
        render_daily_view(params, ss)
    else:
        render_monthly_view(params, ss)
