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
        import plotly.subplots as _psub
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

        def _render_pa_subplot_heatmaps(draw_df, location, view, hour_range,
                                        key):
            """Render all shifts for a location as a single subplots figure.

            Each shift gets its own subplot, vertically stacked, with the
            shift name as a Plotly subplot title (USC maroon, left-aligned).
            Two extra "Draws" / "Samples" columns to the right of every
            hour grid show each tech's total counts (or per-day average in
            Monthly view) on a flat-neutral colorscale that's independent
            of the YlOrBr hour gradient.

            Each subplot title carries hover-only metadata (active techs,
            total draws, total samples for the shift). Hover fires through
            Plotly's annotation hover (`captureevents` + `hovertext`).

            Subplots share `_vmax` (95th-percentile of non-zero values
            across ALL shifts) so the YlOrBr gradient is consistent
            across the location — a "5" cell means the same draw count
            on every shift.

            Each subplot's row weight is `max(n_techs, MIN_TECHS)`, and
            total figure height is `sum(weights) * 28 + chrome`. The
            MIN_TECHS floor (6) ensures every subplot's plot area is
            ≥168 px, comfortably above plotly's findBin ~120 px
            hover-precision threshold. Trade-off: cells in low-tech
            shifts stretch vertically to fill the subplot.
            """
            import calendar as _calp

            _h_start, _h_end = hour_range
            _hours_subset = list(range(_h_start, _h_end + 1))
            _n_hours = len(_hours_subset)

            _shifts = _PA_SHIFT_ORDER.get(location, [None])

            # Subset draw_df once; reused for per-shift slicing.
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

            # n_days for Monthly per-day averaging.
            _n_days = (
                _calp.monthrange(_pa_yr, _pa_mo)[1] if view == "Monthly" else 1
            )

            # Pass 1 — build per-shift bookkeeping (pivot, techs, summary).
            _shift_data = []
            for _shift in _shifts:
                _pivot = build_draw_pivot(
                    draw_df, location, _shift, view,
                    year=_pa_yr, month=_pa_mo,
                )
                _pivot = _pivot[_hours_subset]
                _techs = _pivot.index.tolist()
                if not _techs:
                    continue

                if _shift is None:
                    _shift_df = _loc_hr_df
                else:
                    _shift_df = (
                        _loc_hr_df[_loc_hr_df["shift"] == _shift]
                        if not _loc_hr_df.empty else _loc_hr_df
                    )

                _shift_data.append({
                    "shift":     _shift,
                    "pivot":     _pivot,
                    "techs":     _techs,
                    "n_techs":   len(_techs),
                    "shift_df":  _shift_df,
                    "n_draws":   len(_shift_df),
                    "n_samples": (
                        int(_shift_df["samples"].sum())
                        if not _shift_df.empty else 0
                    ),
                })

            if not _shift_data:
                st.info(
                    f"No data for **{location}** on "
                    f"**{_pa_date_label}** in the selected hour range."
                )
                return

            # Global zmax for consistent YlOrBr gradient across subplots.
            _all_nonzero = [
                v for _d in _shift_data
                for row in _d["pivot"].values
                for v in row
                if v > 0
            ]
            _vmax = (
                float(_np.percentile(_all_nonzero, 95))
                if _all_nonzero else 1.0
            )
            _vmax = max(_vmax, 1.0)

            # Row weights with a floor so short shifts get enough plot
            # area for reliable hover hit-testing.
            _MIN_TECHS = 6
            _row_weights = [
                max(_d["n_techs"], _MIN_TECHS) for _d in _shift_data
            ]
            _n_rows = len(_shift_data)

            # Subplot titles — None shift (HC3) renders as empty string,
            # so no title appears for it.
            _titles = [
                _d["shift"] if _d["shift"] else "" for _d in _shift_data
            ]

            # X-axis tick layout — hour labels + two Total column labels.
            # Numeric x-coords (0..n-1) avoid the categorical heatmap
            # half-width rendering bug; tick labels are mapped back via
            # tickvals/ticktext.
            _x_hour_coords  = list(range(_n_hours))
            _x_draws_coord  = _n_hours
            _x_samples_coord = _n_hours + 1
            _tick_vals = _x_hour_coords + [_x_draws_coord, _x_samples_coord]
            _tick_text = (
                [HOUR_LABELS[h] for h in _hours_subset]
                + ["Draws", "Samples"]
            )

            _fig = _psub.make_subplots(
                rows=_n_rows, cols=1,
                shared_xaxes=False,   # hour labels show on every subplot
                vertical_spacing=0.06,
                subplot_titles=_titles,
                row_heights=_row_weights,
            )

            # Pass 2 — add hours + totals traces per shift.
            for _i, _d in enumerate(_shift_data, start=1):
                _z_arr = _d["pivot"].values.astype(float)
                _z_masked = _np.where(_z_arr == 0, _np.nan, _z_arr).tolist()
                _text_hours = [
                    [str(int(round(v))) if v > 0 else "" for v in row]
                    for row in _z_arr
                ]

                # Per-cell hover detail (Daily = per-draw breakdown,
                # Monthly = avg).
                _details: dict = {}
                if view == "Daily" and not _d["shift_df"].empty:
                    for (_tech, _hour), _grp in _d["shift_df"].groupby(
                        ["display_name", "hour"]
                    ):
                        _grp_sorted = _grp.sort_values("draw_datetime")
                        _n_d_cell = len(_grp_sorted)
                        _n_s_cell = int(_grp_sorted["samples"].sum())
                        _lines = [
                            f"{_n_d_cell} draw{'s' if _n_d_cell != 1 else ''} · "
                            f"{_n_s_cell} total sample"
                            f"{'s' if _n_s_cell != 1 else ''}"
                        ]
                        for _, _r in _grp_sorted.iterrows():
                            _t = pd.to_datetime(
                                _r["draw_datetime"]
                            ).strftime("%H:%M")
                            _s = int(_r["samples"])
                            _lines.append(
                                f"{_t} - {_s} sample"
                                f"{'s' if _s != 1 else ''}"
                            )
                        _details[(_tech, int(_hour))] = "<br>".join(_lines)

                # customdata for hour cells: [hour_label, detail_html]
                # per cell. Hour label must travel via customdata because
                # %{x} would render the numeric coord, not the label.
                _cd_hours = []
                for _tech in _d["techs"]:
                    _row_cd = []
                    for _j, _h in enumerate(_hours_subset):
                        _label = HOUR_LABELS[_h]
                        if view == "Monthly":
                            _v = _d["pivot"].loc[_tech].iloc[_j]
                            _detail = (
                                f"Avg draws: {_v:.1f}"
                                if _v > 0 else "No draws this hour"
                            )
                        else:
                            _detail = _details.get(
                                (_tech, _h), "No draws this hour"
                            )
                        _row_cd.append([_label, _detail])
                    _cd_hours.append(_row_cd)

                # Hours trace — YlOrBr gradient, NaN-masked zeros.
                _fig.add_trace(
                    _pgo.Heatmap(
                        z=_z_masked,
                        x=_x_hour_coords,
                        y=_d["techs"],
                        text=_text_hours,
                        texttemplate="%{text}",
                        hoverinfo="text",
                        colorscale="YlOrBr",
                        zmin=0,
                        zmax=_vmax,
                        xgap=1,
                        ygap=1,
                        showscale=False,
                        customdata=_cd_hours,
                        hovertemplate=(
                            "<b>%{y} @ %{customdata[0]}</b><br>"
                            "%{customdata[1]}<extra></extra>"
                        ),
                        hoverongaps=False,
                    ),
                    row=_i, col=1,
                )

                # Totals trace — two cells per tech (Draws + Samples),
                # flat neutral grey so wide-range totals don't compress
                # the per-hour gradient.
                _z_totals = []
                _text_totals = []
                _cd_totals = []
                for _tech in _d["techs"]:
                    _tech_df = (
                        _d["shift_df"][
                            _d["shift_df"]["display_name"] == _tech
                        ]
                        if not _d["shift_df"].empty else _d["shift_df"]
                    )
                    _n_d_tech = len(_tech_df)
                    _n_s_tech = (
                        int(_tech_df["samples"].sum())
                        if not _tech_df.empty else 0
                    )

                    if view == "Monthly":
                        _d_val = _n_d_tech / max(_n_days, 1)
                        _s_val = _n_s_tech / max(_n_days, 1)
                        _d_txt = f"{_d_val:.1f}" if _n_d_tech > 0 else ""
                        _s_txt = f"{_s_val:.1f}" if _n_s_tech > 0 else ""
                        _d_hover = f"Avg draws/day: {_d_val:.1f}"
                        _s_hover = f"Avg samples/day: {_s_val:.1f}"
                    else:
                        _d_txt = str(_n_d_tech) if _n_d_tech > 0 else ""
                        _s_txt = str(_n_s_tech) if _n_s_tech > 0 else ""
                        _d_hover = (
                            f"{_n_d_tech:,} total draw"
                            f"{'s' if _n_d_tech != 1 else ''}"
                        )
                        _s_hover = (
                            f"{_n_s_tech:,} total sample"
                            f"{'s' if _n_s_tech != 1 else ''}"
                        )

                    _z_totals.append([0, 0])  # flat colorscale, value irrelevant
                    _text_totals.append([_d_txt, _s_txt])
                    _cd_totals.append([
                        ["Total Draws",   _d_hover],
                        ["Total Samples", _s_hover],
                    ])

                _fig.add_trace(
                    _pgo.Heatmap(
                        z=_z_totals,
                        x=[_x_draws_coord, _x_samples_coord],
                        y=_d["techs"],
                        text=_text_totals,
                        texttemplate="%{text}",
                        hoverinfo="text",
                        colorscale=[[0.0, "#ececec"], [1.0, "#ececec"]],
                        zmin=0,
                        zmax=1,
                        xgap=1,
                        ygap=1,
                        showscale=False,
                        customdata=_cd_totals,
                        hovertemplate=(
                            "<b>%{y} @ %{customdata[0]}</b><br>"
                            "%{customdata[1]}<extra></extra>"
                        ),
                        textfont=dict(color="#1a1a1a", size=11),
                    ),
                    row=_i, col=1,
                )

                # Per-subplot axis config — explicit tick labels on each
                # so users don't have to look up which hour a cell is in.
                _fig.update_xaxes(
                    tickmode="array",
                    tickvals=_tick_vals,
                    ticktext=_tick_text,
                    tickfont=dict(size=10),
                    side="bottom",
                    fixedrange=True,
                    showticklabels=True,
                    row=_i, col=1,
                )
                _fig.update_yaxes(
                    tickfont=dict(size=10),
                    autorange="reversed",
                    fixedrange=True,
                    automargin=True,
                    row=_i, col=1,
                )

            # Restyle the subplot title annotations + attach per-shift
            # hover (active techs / draws / samples). `make_subplots`
            # creates one annotation per row with `xref="x{i} domain"`,
            # x=0.5, centered above the subplot — restyle to USC maroon
            # bold, left-aligned. NOTE: annotation hoverlabel only
            # accepts bgcolor/bordercolor/font (not `align`).
            for _i, _ann in enumerate(_fig.layout.annotations):
                if _i >= len(_shift_data):
                    continue
                _d = _shift_data[_i]
                if _d["shift"] is None:
                    continue
                _ann.update(
                    text=f"<b>{_d['shift']}</b>",
                    font=dict(
                        size=13, color="#6F1828",
                        family="Inter, system-ui, sans-serif",
                    ),
                    x=0,
                    xanchor="left",
                    hovertext=(
                        f"<b>{_d['shift']}</b><br>"
                        f"{_d['n_techs']} active tech"
                        f"{'s' if _d['n_techs'] != 1 else ''}<br>"
                        f"{_d['n_draws']:,} total draw"
                        f"{'s' if _d['n_draws'] != 1 else ''}<br>"
                        f"{_d['n_samples']:,} total sample"
                        f"{'s' if _d['n_samples'] != 1 else ''}"
                    ),
                    captureevents=True,
                    hoverlabel=dict(
                        bgcolor="white",
                        bordercolor="#6F1828",
                        font=dict(
                            size=12,
                            family="Inter, system-ui, sans-serif",
                            color="#1a1a1a",
                        ),
                    ),
                )

            # Total figure height: sum(weights) * 28 + chrome per row.
            # Chrome budget per row (~60 px) covers subplot title + x-axis
            # ticks + vertical spacing.
            _PX_PER_TECH = 28
            _PX_CHROME_PER_ROW = 60
            _total_h = (
                sum(_row_weights) * _PX_PER_TECH
                + _PX_CHROME_PER_ROW * _n_rows
                + 40
            )
            _total_h = max(_total_h, 320)

            _fig.update_layout(
                height=_total_h,
                margin=dict(l=10, r=10, t=20, b=20),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                dragmode=False,
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
                key=key,
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

        # Single subplots figure for the whole location — all shifts in
        # one chart, with shift sections demarcated by Plotly subplot
        # titles (USC maroon, hover-only summary). Replaces the previous
        # per-shift `st.plotly_chart` loop; sized via row_heights so each
        # shift's subplot has enough plot area for reliable hover.
        _render_pa_subplot_heatmaps(
            _draw_df, pa_location, pa_view,
            (pa_h_start, pa_h_end),
            f"subplot_heatmaps_{pa_location}",
        )

    except Exception as e:
        st.exception(e)
