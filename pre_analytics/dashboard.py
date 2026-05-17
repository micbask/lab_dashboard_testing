import calendar as _cal
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from config import HOUR_LABELS, PRE_ANALYTICS_LOCATIONS
from storage import (
    storage_is_configured, get_data_summary, ensure_partitioned_storage,
    reset_all_data, delete_date_range, get_index_hash,
)
from ui_components import (
    metric_card, render_header, render_data_management_sidebar,
)
from pre_analytics.data import (
    load_draw_data, build_draw_pivot, build_draw_count_pivot,
)
from pre_analytics.views._shared import (
    render_pa_subplot_heatmaps, render_pa_hourly_bar,
    render_pa_weekday_pattern,
)


def render_sidebar(ss) -> dict:
    """Render pre-analytics sidebar widgets. Returns params dict for render()."""
    # ── URL → session_state hydration ──────────────────────────────────
    # Same pattern as analytics: hydrate ONLY when the widget's
    # session_state key is absent (first render or fresh tab from a
    # shared link). Re-reading on every rerun would clobber the user's
    # click because the URL still holds the OLD value until the END of
    # this function syncs the new one back.
    _qp = st.query_params
    _qp_loc = _qp.get("loc")
    if (
        _qp_loc
        and _qp_loc in PRE_ANALYTICS_LOCATIONS
        and "pa_location_radio" not in st.session_state
    ):
        st.session_state["pa_location_radio"] = _qp_loc
    _qp_view = _qp.get("view")
    if (
        _qp_view in ("Daily", "Monthly")
        and "pa_view_radio" not in st.session_state
    ):
        st.session_state["pa_view_radio"] = _qp_view
    _qp_date = _qp.get("date")
    if _qp_date and "pa_date_picker" not in st.session_state:
        try:
            st.session_state["pa_date_picker"] = date.fromisoformat(_qp_date)
        except ValueError:
            pass

    with st.sidebar:
        # ── Pending background tasks (Issue 2A — DM parity with
        # analytics). Mirror analytics' handler block so that Reset /
        # Delete Range from the pre-analytics Data Management expander
        # actually take effect on this dashboard's session too.
        # `pending_forecast_retrain` is analytics-only (pre-analytics
        # has no forecasts), so we don't handle it here — if a user
        # clicks Refresh Forecast from pre-analytics' DM, the flag sits
        # until they visit analytics, which is the documented behaviour. */
        if ss.pop("pending_reset", False):
            try:
                if storage_is_configured():
                    reset_all_data()
                    st.cache_data.clear()
                    st.success("Master dataset cleared.")
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

        st.markdown(
            '<div class="sidebar-section-label">LOCATION</div>',
            unsafe_allow_html=True,
        )
        pa_location = st.radio(
            "Location", PRE_ANALYTICS_LOCATIONS,
            horizontal=True, label_visibility="collapsed",
            key="pa_location_radio",
        )

        st.markdown(
            '<div class="sidebar-section-label">VIEW</div>',
            unsafe_allow_html=True,
        )
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
            st.markdown(
                '<div class="sidebar-section-label">DATE</div>',
                unsafe_allow_html=True,
            )

            # Prev/Next clicks (below) write `_pa_pending_date` and
            # rerun; pull that into the date_input's session-state
            # key BEFORE the widget renders so st.date_input picks it
            # up as the new value on this render. Mirrors the
            # analytics-dashboard pattern.
            if "_pa_pending_date" in ss:
                _pending = ss.pop("_pa_pending_date")
                if _pa_min_d <= _pending <= _pa_max_d:
                    ss["pa_date_picker"] = _pending

            # Daily and Monthly views keep separate session-state keys
            # (`pa_date_daily` vs `pa_date_monthly`) so toggling between
            # them preserves each view's selection independently. A
            # prior version used a single `pa_date` key shared by both
            # views; switching Monthly -> Daily would then read back
            # "YYYY-MM" and date.fromisoformat() would raise
            # ValueError, silently resetting Daily to today.
            _pa_date_default = ss.get("pa_date_daily", _pa_max_d)
            if isinstance(_pa_date_default, str):
                try:
                    _pa_date_default = date.fromisoformat(_pa_date_default)
                except Exception:
                    _pa_date_default = _pa_max_d
            if (
                not isinstance(_pa_date_default, date)
                or _pa_date_default < _pa_min_d
                or _pa_date_default > _pa_max_d
            ):
                _pa_date_default = _pa_max_d

            # Native st.date_input — built-in min/max enforcement,
            # single-click selection, month / year dropdowns at the top
            # of the calendar popup. Trigger styling (dark fill, white
            # text) is in ui_components CSS; popup uses Streamlit's
            # default light theme.
            pa_date = st.date_input(
                "Select date",
                value=_pa_date_default,
                min_value=_pa_min_d,
                max_value=_pa_max_d,
                label_visibility="collapsed",
                key="pa_date_picker",
            )
            ss["pa_date_daily"] = pa_date
            _pa_date_str = pa_date.isoformat()
            # Date-range metadata caption — sits BELOW the date input,
            # styled small + muted via .sidebar-meta-caption.
            if _pa_data_ok:
                st.markdown(
                    f'<div class="sidebar-meta-caption">'
                    f'{_pa_min_d} → {_pa_max_d}'
                    f'</div>',
                    unsafe_allow_html=True,
                )

            # Prev / Next nav buttons. Equal-width columns with a
            # medium (16 px) gap so the two buttons are visually
            # balanced regardless of sidebar width. Each button
            # stretches to fill its column. Keys are intentionally
            # shared with the analytics dashboard so the CSS in
            # ui_components.py (outline-only look, hover lift,
            # disabled state, .st-key-nav_*_date) applies here too —
            # both dashboards never render simultaneously, so the
            # shared keys do not produce DuplicateWidgetID errors.
            _pa_nc1, _pa_nc2 = st.columns([1, 1], gap="medium")
            with _pa_nc1:
                if st.button(
                    "←",
                    disabled=(pa_date <= _pa_min_d),
                    key="nav_prev_date",
                    width="stretch",
                ):
                    ss["_pa_pending_date"] = pa_date - timedelta(days=1)
                    st.rerun()
            with _pa_nc2:
                if st.button(
                    "→",
                    disabled=(pa_date >= _pa_max_d),
                    key="nav_next_date",
                    width="stretch",
                ):
                    ss["_pa_pending_date"] = pa_date + timedelta(days=1)
                    st.rerun()
        else:
            st.markdown(
                '<div class="sidebar-section-label">MONTH</div>',
                unsafe_allow_html=True,
            )
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
            _pa_month_labels = [f"{_cal.month_name[m]} {y}" for y, m in _pa_avail_months]
            _pa_sel_label = st.selectbox(
                "Select month", _pa_month_labels,
                index=len(_pa_avail_months) - 1,
                label_visibility="collapsed",
                key="pa_month_picker",
            )
            _pa_sel_idx = _pa_month_labels.index(_pa_sel_label)
            _pa_sel_year, _pa_sel_month = _pa_avail_months[_pa_sel_idx]
            _pa_date_str = f"{_pa_sel_year:04d}-{_pa_sel_month:02d}"
            ss["pa_date_monthly"] = _pa_date_str

        # ── Hour Range  (display-only filter; mirrors analytics styling) ──
        st.markdown(
            '<div class="sidebar-section-label">HOUR RANGE</div>',
            unsafe_allow_html=True,
        )

        def _pa_fmt_h(h: int) -> str:
            hr12 = 12 if h % 12 == 0 else h % 12
            suf  = "AM" if h < 12 else "PM"
            return f"{hr12}:00 {suf}"

        pa_hour_range = st.slider(
            "Hours", 0, 23, (0, 23),
            label_visibility="collapsed",
            key="pa_hour_range",
        )
        st.markdown(
            f'<div class="sidebar-meta-caption">'
            f'{_pa_fmt_h(pa_hour_range[0])} → {_pa_fmt_h(pa_hour_range[1])}'
            f'</div>',
            unsafe_allow_html=True,
        )

        # ── Data Management (Issue 2A — shared with analytics). ─────
        # Same shared function as analytics; mirrors the dark expander
        # with the admin-gated body.
        if storage_is_configured():
            _pa_dm_data_summary: dict = {"total_rows": 0, "partitions": 0}
            _pa_dm_load_err: Exception | None = None
            try:
                _pa_dm_data_summary = get_data_summary()
            except Exception as _pa_e:
                _pa_dm_load_err = _pa_e

            render_data_management_sidebar(
                ss,
                data_exists=_pa_data_ok,
                data_summary=_pa_dm_data_summary,
                load_err=_pa_dm_load_err,
            )

    # ── session_state → URL sync ───────────────────────────────────────
    # Only write pre-analytics filters when we're actually on the
    # pre-analytics dashboard (otherwise the analytics sync would
    # clobber these keys). _pa_date_str is "YYYY-MM-DD" for Daily and
    # "YYYY-MM" for Monthly — same value the shared-link consumer will
    # parse back via date.fromisoformat() (for Daily) or year/month
    # split (for Monthly).
    if st.query_params.get("dashboard") == "pre_analytics":
        st.query_params["loc"]  = pa_location
        st.query_params["view"] = pa_view
        if _pa_date_str:
            st.query_params["date"] = _pa_date_str
        elif "date" in st.query_params:
            del st.query_params["date"]

    return {
        "pa_location": pa_location,
        "pa_view": pa_view,
        "pa_date_str": _pa_date_str,
        "pa_hour_range": pa_hour_range,
    }


def render(params: dict, ss) -> None:
    """Render pre-analytics main panel."""
    pa_location  = params["pa_location"]
    pa_view      = params["pa_view"]
    _pa_ds       = params["pa_date_str"]
    pa_h_start, pa_h_end = params.get("pa_hour_range", (0, 23))

    try:

        # Parse the active date string into year/month so the
        # Monthly-view per-day averaging in build_draw_pivot uses
        # calendar days rather than days-with-data.
        if len(_pa_ds) == 7:
            import calendar as _calpa
            _pa_yr, _pa_mo = int(_pa_ds[:4]), int(_pa_ds[5:7])
            _pa_date_label = f"{_calpa.month_name[_pa_mo]} {_pa_yr}"
        else:
            _pa_d = pd.Timestamp(_pa_ds).to_pydatetime().date()
            _pa_yr, _pa_mo = _pa_d.year, _pa_d.month
            _pa_date_label = pd.Timestamp(_pa_ds).strftime("%B %d, %Y")

        render_header(pa_location, _pa_date_label)

        _idx_hash = get_index_hash() if storage_is_configured() else ""
        _draw_df = load_draw_data(_pa_ds, pa_view, index_hash=_idx_hash)

        # KPI cards reflect the selected hour range: filter both by
        # location and by hour ∈ [pa_h_start, pa_h_end].
        if not _draw_df.empty:
            _loc_df = _draw_df[
                (_draw_df["location"] == pa_location)
                & (_draw_df["hour"] >= pa_h_start)
                & (_draw_df["hour"] <= pa_h_end)
            ]
        else:
            _loc_df = _draw_df
        _pa_total_draws   = len(_loc_df)
        _pa_total_samples = int(_loc_df["samples"].sum()) if not _loc_df.empty else 0
        _pa_active_techs  = int(_loc_df["display_name"].nunique()) if not _loc_df.empty else 0
        if not _loc_df.empty:
            _peak_h_val   = int(_loc_df.groupby("hour").size().idxmax())
            _pa_peak_hour = HOUR_LABELS.get(_peak_h_val, str(_peak_h_val))
        else:
            _pa_peak_hour = "-"

        _kc1, _kc2, _kc3, _kc4 = st.columns(4)
        with _kc1:
            st.markdown(metric_card("Total draws", f"{_pa_total_draws:,}", accent=True),
                        unsafe_allow_html=True)
        with _kc2:
            st.markdown(metric_card("Total samples", f"{_pa_total_samples:,}"),
                        unsafe_allow_html=True)
        with _kc3:
            st.markdown(metric_card("Active techs", str(_pa_active_techs)),
                        unsafe_allow_html=True)
        with _kc4:
            st.markdown(metric_card("Peak hour", _pa_peak_hour), unsafe_allow_html=True)

        st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)


        # Shared section header + colourscale legend, rendered ONCE above
        # all the per-shift heatmaps (matches the analytics dashboard's
        # "Completed Volume by Procedure & Hour" header / legend pair).
        # The YlOrBr swatch colours are the actual low/high stops of the
        # Plotly built-in colorscale used on the heatmaps below.
        # NOTE: no section-level "N = X days" here because Monthly view
        # uses PER-TECH active-day denominators (different N per row),
        # so a single section-level N would be misleading. The N for
        # each tech is surfaced in the cell hover instead.
        st.markdown(
            '<div class="section-heading">Draws by tech &amp; hour</div>',
            unsafe_allow_html=True,
        )
        # View-aware legend prefix — Monthly cells are averages (avg
        # draws per active day for that tech / hour); Daily cells are
        # exact counts.
        if pa_view == "Monthly":
            _legend_values = "Values = avg draws per day in hour. "
        else:
            _legend_values = "Values = draws per hour. "
        st.markdown(
            f'<div class="heatmap-legend">'
            f'{_legend_values}'
            f'Colour scale: &nbsp;'
            f'<strong style="color:#fff7bc;">■</strong> low &nbsp;→&nbsp; '
            f'<strong style="color:#8c2d04;">■</strong> high'
            f'</div>',
            unsafe_allow_html=True,
        )

        # Single subplots figure for the whole location — all shifts in
        # one chart, with shift sections demarcated by Plotly subplot
        # titles (USC maroon, hover-only summary). Replaces the previous
        # per-shift `st.plotly_chart` loop; sized via absolute pixel
        # yaxis.domain overrides so every subplot's plot area is exactly
        # n_techs * 28 px (uniform cells across shifts).
        render_pa_subplot_heatmaps(
            _draw_df, pa_location, pa_view,
            (pa_h_start, pa_h_end),
            _pa_yr, _pa_mo,
            f"subplot_heatmaps_{pa_location}",
        )

        # ── Volume patterns section ──────────────────────────────────
        # Second chart group, sits below the heatmap. Daily view shows
        # an hourly bar chart of total draws; Monthly view shows a
        # weekday × hour heatmap with avg-draws-per-occurrence cells.
        # Both restrict to the selected location and hour range, and
        # surface total active techs / draws / samples on hover.
        st.markdown("---")

        if pa_view == "Daily":
            render_pa_hourly_bar(
                _draw_df, pa_location,
                (pa_h_start, pa_h_end),
                _pa_date_label,
                key=f"pa_hourly_bar_{pa_location}",
            )
        else:
            render_pa_weekday_pattern(
                _draw_df, pa_location,
                (pa_h_start, pa_h_end),
                _pa_yr, _pa_mo, _pa_date_label,
                key=f"pa_weekday_pattern_{pa_location}",
            )

    except Exception as e:
        st.exception(e)
