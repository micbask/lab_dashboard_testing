import calendar as _cal
from datetime import date, timedelta

import pandas as pd
import streamlit as st

from config import HOUR_LABELS
from storage import storage_is_configured, get_data_summary, ensure_partitioned_storage
from ui_components import metric_card, render_header
from pre_analytics.data import load_phlebotomy_staff, load_draw_data, build_draw_pivot


def render_sidebar(ss) -> dict:
    """Render pre-analytics sidebar widgets. Returns params dict for render()."""
    with st.sidebar:
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
            _pa_date_default = ss.get("pa_date", _pa_max_d)
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
            ss["pa_date"] = pa_date
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
            ss["pa_date"] = _pa_date_str

    return {
        "pa_location": pa_location,
        "pa_view": pa_view,
        "pa_date_str": _pa_date_str,
    }


def render(params: dict, ss) -> None:
    """Render pre-analytics main panel."""
    pa_location = params["pa_location"]
    pa_view     = params["pa_view"]
    _pa_ds      = params["pa_date_str"]

    try:
        import plotly.graph_objects as _pgo
        import numpy as _np

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
            st.write("Staff lookup keys — first 10 (normalized):",
                     _draw_debug.get("staff_keys", []))
            st.write("Parquet Drawn Tech — first 10 raw values:",
                     _draw_debug.get("raw_drawn_tech", []))
            st.write("Rows before name-match filter:", _draw_debug.get("rows_before", 0))
            st.write("Rows after name-match filter:", _draw_debug.get("rows_after", 0))

        _loc_df = (
            _draw_df[_draw_df["location"] == pa_location]
            if not _draw_df.empty else _draw_df
        )
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

            _flat    = [v for row in _z for v in row if v > 0]
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
                hoverinfo="none",
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

            _cell_key = f"selected_cell_{heatmap_key}"
            _sel_event = st.plotly_chart(
                _fig,
                use_container_width=True,
                on_select="rerun",
                key=heatmap_key,
            )

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
                        _current = ss.get(_cell_key)
                        # Toggle: same cell clicked again → clear selection.
                        if (
                            _current
                            and _current["tech"] == _sel_tech
                            and _current["hour"] == _sel_hour
                        ):
                            del ss[_cell_key]
                        else:
                            ss[_cell_key] = {
                                "tech": _sel_tech,
                                "hour": _sel_hour,
                                "hlabel": _sel_hlabel,
                            }

            _stored = ss.get(_cell_key)
            if _stored:
                _d_tech   = _stored["tech"]
                _d_hour   = _stored["hour"]
                _d_hlabel = _stored["hlabel"]

                _detail = (
                    draw_df[
                        (draw_df["display_name"] == _d_tech) &
                        (draw_df["hour"] == _d_hour)
                    ]
                    .sort_values("draw_datetime")
                    .copy()
                    if not draw_df.empty else pd.DataFrame()
                )

                _sep = "─" * 35
                with st.container(border=True):
                    st.markdown(f"**{_d_tech} @ {_d_hlabel}**")
                    st.markdown(f"`{_sep}`")
                    if _detail.empty:
                        st.caption("No draws found.")
                    else:
                        _lines = []
                        for _, _r in _detail.iterrows():
                            _t = pd.to_datetime(_r["draw_datetime"]).strftime("%H:%M")
                            _s = int(_r["samples"])
                            _lines.append(
                                f"`{_t}`  &nbsp;—&nbsp;  {_s} sample{'s' if _s != 1 else ''}"
                            )
                        st.markdown("  \n".join(_lines), unsafe_allow_html=True)
                        st.markdown(f"`{_sep}`")
                        _n_draws = len(_detail)
                        _n_samps = int(_detail["samples"].sum())
                        st.markdown(
                            f"**{_n_draws} draw{'s' if _n_draws != 1 else ''}** "
                            f"&nbsp;·&nbsp; "
                            f"**{_n_samps} total sample{'s' if _n_samps != 1 else ''}**",
                            unsafe_allow_html=True,
                        )

        for _pa_shift in _PA_SHIFT_ORDER.get(pa_location, [None]):
            if pa_location != "HC3" and _pa_shift is not None:
                st.subheader(f"{pa_location} — {_pa_shift}")
            _hkey = f"heatmap_{pa_location}_{_pa_shift or 'all'}"
            _render_pa_heatmap(_draw_df, pa_location, _pa_shift, pa_view, _hkey)

    except Exception as e:
        st.exception(e)
