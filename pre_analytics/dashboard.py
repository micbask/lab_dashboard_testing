import calendar as _cal
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from config import HOUR_LABELS
from storage import (
    storage_is_configured, get_data_summary, ensure_partitioned_storage,
    reset_all_data, delete_date_range, get_index_hash,
)
from ui_components import (
    metric_card, render_header, render_data_management_sidebar,
)
from pre_analytics.data import load_draw_data, build_draw_pivot


def render_sidebar(ss) -> dict:
    """Render pre-analytics sidebar widgets. Returns params dict for render()."""
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
            "Location", ["Keck", "Norris", "HC3"],
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
        import plotly.graph_objects as _pgo
        import numpy as _np

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
        _draw_df, _draw_debug = load_draw_data(_pa_ds, pa_view, index_hash=_idx_hash)

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

        _PA_HOUR_LABELS = [HOUR_LABELS[h] for h in range(24)]

        _PA_SHIFT_ORDER = {
            "Keck":   ["Early AM", "AM", "PM", "NS"],
            "Norris": ["AM", "PM", "NS"],
            "HC3":    [None],
        }

        def _render_combined_heatmap(draw_df, location, view, hour_range,
                                     heatmap_key):
            """Render a single combined heatmap for the location.

            All techs from all shifts are laid out in one Plotly figure
            ordered by `_PA_SHIFT_ORDER[location]`. Shifts are demarcated
            visually with:
              • thin horizontal divider lines between shift sections
              • a USC-maroon shift-name annotation rendered vertically
                at the figure's left edge, with the per-shift summary
                (draws · samples · tech count) surfaced on hover

            A right-side "Total" column shows the per-tech daily total
            (Daily view) or per-day average (Monthly view). Like the
            analytics dashboard, the Total column is a SEPARATE trace
            with its own flat-neutral colorscale so the wide-range total
            values don't compress the per-hour gradient. Total cell
            tooltip surfaces both total draws AND total samples for the
            tech.

            Combining all shifts into one chart eliminates the short-
            chart hover precision bug that plagued per-shift charts on
            shifts with 1-3 techs: total chart height is now
            (n_total_techs × 28 + chrome), always large enough for
            plotly's findBin hit-test to work reliably.
            """
            _h_start, _h_end = hour_range
            _hours_subset = list(range(_h_start, _h_end + 1))
            _n_hour_cols  = len(_hours_subset)

            _shifts = _PA_SHIFT_ORDER.get(location, [None])

            # Subset to this location once; reused for shift summaries
            # and per-cell daily-view details.
            _loc_df = (
                draw_df[draw_df["location"] == location]
                if not draw_df.empty else draw_df
            )
            _loc_hr_df = (
                _loc_df[
                    (_loc_df["hour"] >= _h_start)
                    & (_loc_df["hour"] <= _h_end)
                ]
                if not _loc_df.empty else _loc_df
            )

            # n_days for Monthly per-day averaging (matches what
            # build_draw_pivot uses internally).
            import calendar as _calp
            _n_days = _calp.monthrange(_pa_yr, _pa_mo)[1] if view == "Monthly" else 1

            # Pass 1 — build the combined tech ordering + per-shift bookkeeping.
            _combined_techs: list = []
            _shift_info: list = []   # list of dicts: shift / start_idx / end_idx / etc.
            _z_hours_rows: list = []
            for _shift in _shifts:
                _pivot = build_draw_pivot(
                    draw_df, location, _shift, view,
                    year=_pa_yr, month=_pa_mo,
                )
                _pivot = _pivot[_hours_subset]
                _techs = _pivot.index.tolist()
                if not _techs:
                    continue

                _start = len(_combined_techs)
                _combined_techs.extend(_techs)
                _end = len(_combined_techs)

                for _tech in _techs:
                    _z_hours_rows.append(_pivot.loc[_tech].tolist())

                # Shift-level summary respects the active hour range so
                # the right-margin tally matches the KPI cards above.
                if _shift is None:
                    _shift_df = _loc_hr_df
                else:
                    _shift_df = (
                        _loc_hr_df[_loc_hr_df["shift"] == _shift]
                        if not _loc_hr_df.empty else _loc_hr_df
                    )
                _shift_n_draws = len(_shift_df)
                _shift_n_samples = int(_shift_df["samples"].sum()) if not _shift_df.empty else 0

                _shift_info.append({
                    "shift":     _shift,
                    "start_idx": _start,
                    "end_idx":   _end,
                    "n_techs":   _end - _start,
                    "n_draws":   _shift_n_draws,
                    "n_samples": _shift_n_samples,
                })

            if not _combined_techs:
                st.info(
                    f"No data for **{location}** on **{_pa_date_label}** in "
                    f"the selected hour range."
                )
                return

            _n_tot_techs = len(_combined_techs)

            # zmax keyed to non-zero hour-cell values only — Total column
            # has its own colorscale so wide-range totals can't pull the
            # gradient off-scale.
            _flat = [v for row in _z_hours_rows for v in row if v > 0]
            _vmax_pa = float(_np.percentile(_flat, 95)) if _flat else 1.0
            _vmax_pa = max(_vmax_pa, 1.0)

            # Per-hour-cell text labels (integer count or empty string).
            _text_hours = [
                [str(int(round(v))) if v > 0 else "" for v in row]
                for row in _z_hours_rows
            ]

            # Per-tech Total column = sum of filtered hours (Daily) or
            # sum of avg-per-day per-hour values (Monthly, which is the
            # avg total per day).
            _z_total_rows = [[sum(row)] for row in _z_hours_rows]
            _text_total = []
            for [v] in _z_total_rows:
                if v <= 0:
                    _text_total.append([""])
                elif view == "Monthly":
                    _text_total.append([f"{v:.1f}"])
                else:
                    _text_total.append([str(int(round(v)))])

            # NaN-mask zero hour cells so hoverongaps=False can suppress
            # tooltips on empty cells. The Total trace keeps numeric
            # zeros — its flat colorscale renders zero the same as any
            # value, and hovertemplate handles the "no draws" case.
            _z_hours_masked = [
                [_np.nan if v == 0 else v for v in row]
                for row in _z_hours_rows
            ]

            # Per-cell hover details: Daily view shows the per-draw
            # breakdown sourced from the raw draw_df; Monthly view shows
            # the avg-per-day figure pulled from the pivot.
            _details: dict = {}
            if view == "Daily" and not _loc_df.empty:
                for (_tech, _hour), _grp in _loc_df.groupby(
                    ["display_name", "hour"]
                ):
                    if _hour < _h_start or _hour > _h_end:
                        continue
                    _grp_sorted = _grp.sort_values("draw_datetime")
                    _n_d = len(_grp_sorted)
                    _n_s = int(_grp_sorted["samples"].sum())
                    _lines = [
                        f"{_n_d} draw{'s' if _n_d != 1 else ''} · "
                        f"{_n_s} total sample{'s' if _n_s != 1 else ''}"
                    ]
                    for _, _r in _grp_sorted.iterrows():
                        _t = pd.to_datetime(_r["draw_datetime"]).strftime("%H:%M")
                        _s = int(_r["samples"])
                        _lines.append(
                            f"{_t} - {_s} sample{'s' if _s != 1 else ''}"
                        )
                    _details[(_tech, int(_hour))] = "<br>".join(_lines)

            # Per-tech totals for the Total column hover. Daily: counts
            # are absolute. Monthly: divide by days-in-month to express
            # the same per-day-average semantics the cell text shows.
            _tech_totals: dict = {}
            if not _loc_hr_df.empty:
                for _tech, _grp in _loc_hr_df.groupby("display_name"):
                    _tech_totals[_tech] = (
                        len(_grp),
                        int(_grp["samples"].sum()),
                    )

            # customdata for hour cells: [hour_label, detail_html]. The
            # numeric x-coords (0..N-1) require us to surface the hour
            # label via customdata because %{x} would render as the
            # integer index.
            _cd_hours = []
            for _i, _tech in enumerate(_combined_techs):
                _row = []
                for _j, _h in enumerate(_hours_subset):
                    _label = HOUR_LABELS[_h]
                    if view == "Monthly":
                        _v = _z_hours_rows[_i][_j]
                        _detail = f"Avg draws: {_v:.1f}" if _v > 0 else "No draws this hour"
                    else:
                        _detail = _details.get(
                            (_tech, _h),
                            "No draws this hour",
                        )
                    _row.append([_label, _detail])
                _cd_hours.append(_row)

            # customdata for Total cells: one [label, detail_html] per
            # tech. Detail surfaces total draws and total samples (Daily)
            # or per-day averages (Monthly).
            _cd_total = []
            for _tech in _combined_techs:
                _n_d, _n_s = _tech_totals.get(_tech, (0, 0))
                if view == "Monthly":
                    _avg_d = _n_d / max(_n_days, 1)
                    _avg_s = _n_s / max(_n_days, 1)
                    _detail = (
                        f"{_avg_d:.1f} avg draws/day · "
                        f"{_avg_s:.1f} avg samples/day"
                    )
                else:
                    _detail = (
                        f"{_n_d:,} draw{'s' if _n_d != 1 else ''} · "
                        f"{_n_s:,} total sample{'s' if _n_s != 1 else ''}"
                    )
                _cd_total.append([["Total", _detail]])

            # Numeric x-coords let the hour heatmap and the Total heatmap
            # share a single x-axis without the categorical-side-by-side
            # half-width rendering bug. Tick labels are restored manually
            # via tickvals/ticktext below.
            _x_hours_coords = list(range(_n_hour_cols))
            _x_total_coord  = _n_hour_cols

            _fig = _pgo.Figure()

            # Hour-columns trace: YlOrBr gradient, NaN-masked zeros.
            _fig.add_trace(_pgo.Heatmap(
                z=_z_hours_masked,
                x=_x_hours_coords,
                y=_combined_techs,
                text=_text_hours,
                texttemplate="%{text}",
                hoverinfo="text",
                colorscale="YlOrBr",
                zmin=0,
                zmax=_vmax_pa,
                xgap=1,
                ygap=1,
                showscale=False,
                customdata=_cd_hours,
                hovertemplate=(
                    "<b>%{y} @ %{customdata[0]}</b><br>"
                    "%{customdata[1]}<extra></extra>"
                ),
            ))

            # Total-column trace: flat neutral grey, independent of the
            # hour gradient. Same y-axis (categorical techs) so rows
            # line up exactly with the hour grid.
            _fig.add_trace(_pgo.Heatmap(
                z=_z_total_rows,
                x=[_x_total_coord],
                y=_combined_techs,
                text=_text_total,
                texttemplate="%{text}",
                hoverinfo="text",
                # 2-stop flat colorscale — every total cell renders as the
                # same neutral grey regardless of magnitude.
                colorscale=[[0.0, "#ececec"], [1.0, "#ececec"]],
                zmin=0,
                zmax=1,
                xgap=1,
                ygap=1,
                showscale=False,
                customdata=_cd_total,
                hovertemplate=(
                    "<b>%{y} @ %{customdata[0]}</b><br>"
                    "%{customdata[1]}<extra></extra>"
                ),
                textfont=dict(color="#1a1a1a", size=11),
            ))

            _fig.update_traces(hoverongaps=False)

            # X-axis tick labels: hour names for the gradient cells +
            # "Total" for the right-most cell.
            _tick_vals = list(_x_hours_coords) + [_x_total_coord]
            _tick_text = [HOUR_LABELS[h] for h in _hours_subset] + ["Total"]

            # Shapes — thin 1 px divider lines between shift sections.
            # The alternating low-opacity background bands per shift
            # were removed per design feedback; the dividers alone give
            # enough visual separation.
            _shapes = []
            for _idx, _info in enumerate(_shift_info):
                if _idx < len(_shift_info) - 1:
                    _shapes.append(dict(
                        type="line",
                        xref="x", x0=-0.5, x1=_x_total_coord + 0.5,
                        yref="y",
                        y0=_info["end_idx"] - 0.5,
                        y1=_info["end_idx"] - 0.5,
                        line=dict(color="rgba(0, 0, 0, 0.18)", width=1),
                    ))

            # Annotations — vertical shift label on the LEFT edge of
            # the figure container, with the per-shift summary
            # (draws · samples · tech count) surfaced on hover.
            # xref="container" pins the annotation to the figure's
            # left edge, independent of the plot's automargin-expanded
            # left margin, so the rotated label can never collide with
            # the tech-name tick labels (which live in the automargin
            # zone between the plot area and the figure edge).
            # HC3's None-shift skips the annotation.
            _annotations = []
            for _info in _shift_info:
                if _info["shift"] is None:
                    continue
                _mid_y = (_info["start_idx"] + _info["end_idx"] - 1) / 2.0
                _annotations.append(dict(
                    xref="container", x=0.01, xanchor="left",
                    yref="y", y=_mid_y, yanchor="middle",
                    text=f"<b>{_info['shift']}</b>",
                    textangle=-90,
                    showarrow=False,
                    font=dict(
                        size=12, color="#6F1828",
                        family="Inter, system-ui, sans-serif",
                    ),
                    hovertext=(
                        f"<b>{_info['shift']}</b><br>"
                        f"{_info['n_draws']:,} draws · "
                        f"{_info['n_samples']:,} samples<br>"
                        f"{_info['n_techs']} tech"
                        f"{'s' if _info['n_techs'] != 1 else ''}"
                    ),
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor="#6F1828",
                        font=dict(
                            size=12,
                            family="Inter, system-ui, sans-serif",
                            color="#1a1a1a",
                        ),
                        align="left",
                    ),
                    captureevents=True,
                ))

            # Chart height — grows with total tech count, comfortably
            # above plotly's findBin hover-precision threshold for every
            # location (Keck has ~24 techs across 4 shifts → ~700 px;
            # Norris ~17 → ~520 px; HC3 ~10 → ~360 px).
            _plot_h = _n_tot_techs * 28 + 80
            _fig.update_layout(
                height=_plot_h,
                # l=30 leaves a thin strip at the figure's left edge
                # for the rotated shift-label annotation (xref="container"),
                # then yaxis automargin expands the margin further to fit
                # the tech-name tick labels — the annotation sits in the
                # safe zone outside automargin's range. r=20 since the
                # per-shift tally has been moved into the shift label's
                # hover tooltip.
                margin=dict(l=30, r=20, t=20, b=40),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                dragmode=False,
                shapes=_shapes,
                annotations=_annotations,
                xaxis=dict(
                    tickmode="array",
                    tickvals=_tick_vals,
                    ticktext=_tick_text,
                    tickfont=dict(size=10),
                    side="bottom",
                    fixedrange=True,
                ),
                yaxis=dict(
                    tickfont=dict(size=10),
                    autorange="reversed",
                    fixedrange=True,
                    automargin=True,
                ),
                hoverlabel=dict(
                    bgcolor="white",
                    bordercolor="#6F1828",
                    font=dict(
                        size=12,
                        family="Inter, system-ui, sans-serif",
                        color="#1a1a1a",
                    ),
                    align="left",
                ),
            )

            st.plotly_chart(
                _fig,
                use_container_width=True,
                key=heatmap_key,
                config={
                    "staticPlot": False,
                    "scrollZoom": False,
                    "displayModeBar": False,
                },
            )

        # Shared section header + colourscale legend, rendered ONCE above
        # all the per-shift heatmaps (matches the analytics dashboard's
        # "Completed Volume by Procedure & Hour" header / legend pair).
        # The YlOrBr swatch colours are the actual low/high stops of the
        # Plotly built-in colorscale used on the heatmaps below.
        st.markdown(
            '<div class="section-heading">Draws by tech &amp; hour</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            '<div class="heatmap-legend">'
            'Colour scale: &nbsp;'
            '<strong style="color:#fff7bc;">■</strong> low &nbsp;→&nbsp; '
            '<strong style="color:#8c2d04;">■</strong> high. &nbsp;'
            'Higher values indicate more draws per hour.'
            '</div>',
            unsafe_allow_html=True,
        )

        # Single combined heatmap for the whole location — all techs
        # from every shift in one chart, with shift sections demarcated
        # via in-chart annotations and divider lines. Replaces the
        # previous per-shift loop (one separate plotly chart per shift)
        # which hit a heatmap hover hit-test precision bug on shifts
        # with 1-3 techs because their chart was too short (~56-96 px)
        # for plotly's findBin to resolve the cursor reliably. Combining
        # all shifts into a single tall chart sidesteps that entirely.
        _render_combined_heatmap(
            _draw_df, pa_location, pa_view,
            (pa_h_start, pa_h_end),
            f"combined_heatmap_{pa_location}",
        )

    except Exception as e:
        st.exception(e)
