import calendar as _cal
from datetime import date, datetime, timedelta

import pandas as pd
import streamlit as st
import streamlit_shadcn_ui as ui

from config import HOUR_LABELS
from storage import storage_is_configured, get_data_summary, ensure_partitioned_storage
from ui_components import metric_card, render_header
from pre_analytics.data import load_phlebotomy_staff, load_draw_data, build_draw_pivot


def _coerce_to_date(val):
    """Coerce a shadcn date_picker return value to a datetime.date.

    The library passes its `value` arg straight through to JSON
    marshalling with no type coercion of its own, so we feed it an ISO
    string (the only JSON-safe option) and have to handle whatever the
    JS side sends back: a datetime, a date, an ISO string (with or
    without time / timezone), or None.
    """
    if val is None:
        return None
    if isinstance(val, datetime):
        return val.date()
    if isinstance(val, date):
        return val
    if isinstance(val, str):
        try:
            return datetime.fromisoformat(val.replace("Z", "+00:00")).date()
        except (ValueError, TypeError):
            pass
        try:
            return date.fromisoformat(val[:10])
        except (ValueError, TypeError):
            pass
    return None


def render_sidebar(ss) -> dict:
    """Render pre-analytics sidebar widgets. Returns params dict for render()."""
    with st.sidebar:
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

            # Resolve / clamp the default date from any prior session
            # state value (may be a date or an ISO string) into a date
            # inside the valid [min, max] window.
            _pa_date_default = ss.get("pa_date", _pa_max_d)
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

            # shadcn date picker — light-themed by design; we accept the
            # library's native styling.
            #
            # The library passes `default_value` straight through to
            # JSON marshalling with no type coercion (verified by
            # reading the v0.1.19 source). date / datetime aren't JSON-
            # serializable so they blow up the marshaller; we send an
            # ISO date string and parse the return via _coerce_to_date.
            #
            # The library's init_session writes the seed value to a
            # session_state slot only once per session. A stale date
            # object stored under the previous "pa_date_picker" key
            # from a prior deploy would otherwise keep getting re-read
            # and re-marshalled. Use a fresh key prefix
            # ("pa_date_dp_iso") so the picker initialises clean.
            _picked = ui.date_picker(
                key="pa_date_dp_iso",
                mode="single",
                label="",
                default_value=_pa_date_default.isoformat(),
            )
            _picked_d = _coerce_to_date(_picked)
            if (
                _picked_d is not None
                and _pa_min_d <= _picked_d <= _pa_max_d
            ):
                pa_date = _picked_d
            else:
                pa_date = _pa_date_default

            ss["pa_date"] = pa_date
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
        else:
            import calendar as _cal3
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
            ss["pa_date"] = _pa_date_str

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

        if len(_pa_ds) == 7:
            import calendar as _calpa
            _pa_yr, _pa_mo = int(_pa_ds[:4]), int(_pa_ds[5:7])
            _pa_date_label = f"{_calpa.month_name[_pa_mo]} {_pa_yr}"
        else:
            _pa_date_label = pd.Timestamp(_pa_ds).strftime("%B %d, %Y")

        render_header(pa_location, _pa_date_label)

        _draw_df, _draw_debug = load_draw_data(_pa_ds, pa_view)

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

        def _render_pa_heatmap(draw_df, location, shift, view, heatmap_key,
                               hour_range):
            _h_start, _h_end = hour_range
            _hours_subset = list(range(_h_start, _h_end + 1))

            # Build the full 0..23 pivot then slice to the selected hours.
            _pivot = build_draw_pivot(draw_df, location, shift, view)
            _pivot = _pivot[_hours_subset]
            _techs = _pivot.index.tolist()
            _x     = [HOUR_LABELS[h] for h in _hours_subset]

            _z_arr   = _pivot.values.astype(float)
            _flat    = [v for row in _z_arr for v in row if v > 0]
            _vmax_pa = float(_np.percentile(_flat, 95)) if _flat else 1.0
            _vmax_pa = max(_vmax_pa, 1.0)

            _text_vals = [
                [str(int(round(v))) if v > 0 else "" for v in row]
                for row in _z_arr
            ]

            # Mask zero cells to NaN so hoverongaps=False can suppress the
            # tooltip on empty cells.
            _z = _np.where(_z_arr == 0, _np.nan, _z_arr).tolist()

            _heatmap_kwargs = dict(
                z=_z,
                x=_x,
                y=_techs,
                text=_text_vals,
                texttemplate="%{text}",
                hoverinfo="text",
                colorscale="YlOrBr",
                zmin=0,
                zmax=_vmax_pa,
                xgap=1,
                ygap=1,
                showscale=False,
            )

            if view == "Monthly":
                # Monthly cells are per-day averages — show only the average,
                # no per-draw breakdown.
                _heatmap_kwargs["hovertemplate"] = (
                    "<b>%{y} @ %{x}</b><br>Avg draws: %{z:.1f}<extra></extra>"
                )
            else:
                # Daily — build per-cell draw breakdown for the customdata tooltip.
                if shift is None:
                    _sub = (
                        draw_df[draw_df["location"] == location]
                        if not draw_df.empty else draw_df
                    )
                else:
                    _sub = (
                        draw_df[
                            (draw_df["location"] == location)
                            & (draw_df["shift"] == shift)
                        ] if not draw_df.empty else draw_df
                    )

                _details: dict = {}
                if not _sub.empty:
                    for (_tech, _hour), _grp in _sub.groupby(
                        ["display_name", "hour"]
                    ):
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
                                f"{_t} — {_s} sample{'s' if _s != 1 else ''}"
                            )
                        _details[(_tech, int(_hour))] = "<br>".join(_lines)

                _heatmap_kwargs["customdata"] = [
                    [_details.get((_tech, _h), None) for _h in _hours_subset]
                    for _tech in _techs
                ]
                _heatmap_kwargs["hovertemplate"] = (
                    "<b>%{y} @ %{x}</b><br>%{customdata}<extra></extra>"
                )

            _fig = _pgo.Figure(data=_pgo.Heatmap(**_heatmap_kwargs))
            _fig.update_traces(hoverongaps=False)

            _plot_h = max(180, len(_techs) * 45 + 100)
            _fig.update_layout(
                height=_plot_h,
                margin=dict(l=10, r=10, t=10, b=10),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                dragmode=False,
                xaxis=dict(
                    tickfont=dict(size=10), side="bottom", fixedrange=True,
                ),
                yaxis=dict(
                    tickfont=dict(size=11), autorange="reversed",
                    fixedrange=True,
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
            '<div class="section-heading">Draws by Tech &amp; Hour</div>',
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

        for _pa_shift in _PA_SHIFT_ORDER.get(pa_location, [None]):
            if pa_location != "HC3" and _pa_shift is not None:
                st.subheader(f"{pa_location} — {_pa_shift}")
            _hkey = f"heatmap_{pa_location}_{_pa_shift or 'all'}"
            _render_pa_heatmap(
                _draw_df, pa_location, _pa_shift, pa_view, _hkey,
                (pa_h_start, pa_h_end),
            )

    except Exception as e:
        st.exception(e)
