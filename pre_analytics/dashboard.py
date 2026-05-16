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

        def _render_pa_heatmap(draw_df, location, shift, view, heatmap_key,
                               hour_range):
            _h_start, _h_end = hour_range
            _hours_subset = list(range(_h_start, _h_end + 1))

            # Build the full 0..23 pivot then slice to the selected hours.
            # Pass year/month so Monthly view divides by calendar days
            # (not days-with-data, which would overstate sparse months).
            _pivot = build_draw_pivot(
                draw_df, location, shift, view,
                year=_pa_yr, month=_pa_mo,
            )
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

            # Use uniform numeric z (no NaN masking). NaN cells trigger
            # plotly's heatmap `findBin` hit-test to early-exit via the
            # `hoverongaps:false` branch, and on short charts (1-3 tech
            # rows = 56-84 px plot area) sub-pixel rounding in
            # `xaxis.c2p` / `p2c` can snap the cursor's logical position
            # into a NaN neighbour even when it's visibly over a coloured
            # brick — producing the "I see the cell, hover doesn't fire"
            # symptom users reported on Early-AM / NS shifts. Replacing
            # the NaN mask with a numeric 0 + per-cell `hovertext = ""`
            # for empty cells keeps tooltip suppression but eliminates
            # the findBin trap. See plotly.js
            # src/traces/heatmap/hover.js: `if (zVal === undefined &&
            # !trace.hoverongaps) return;`
            _z = _z_arr.tolist()

            # Build per-cell hovertext: EVERY cell gets a non-empty
            # string. Both prior fixes (v1: NaN mask + hoverongaps=False,
            # v2: numeric z + empty hovertext on empty cells) failed
            # because plotly's `findBin` hit-test can snap the cursor to
            # an adjacent cell when sub-pixel rounding in c2p/p2c
            # produces a 1-pixel error — and if that adjacent cell's
            # tooltip is "suppressed" (either via the hoverongaps
            # early-exit or via empty hovertext), no tooltip fires even
            # though the cursor is visually over a coloured brick. By
            # giving every cell non-empty text, the snap is harmless:
            # whichever cell findBin lands on, a tooltip fires. Non-
            # empty cells get the full draw breakdown; empty cells get
            # a minimal "no draws this hour" tooltip so the user gets
            # consistent feedback when moving the cursor across the
            # chart.
            if view == "Monthly":
                _hovertext = [
                    [
                        (
                            f"<b>{_tech} @ {_x[_j]}</b><br>"
                            f"Avg draws: {_z_arr[_i, _j]:.1f}"
                        )
                        if _z_arr[_i, _j] > 0
                        else f"<b>{_tech} @ {_x[_j]}</b><br>No draws this hour"
                        for _j in range(len(_hours_subset))
                    ]
                    for _i, _tech in enumerate(_techs)
                ]
            else:
                # Daily — build per-cell draw breakdown then format the
                # full tooltip string per cell.
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
                            f"<b>{_tech} @ {HOUR_LABELS[int(_hour)]}</b>",
                            f"{_n_d} draw{'s' if _n_d != 1 else ''} · "
                            f"{_n_s} total sample{'s' if _n_s != 1 else ''}",
                        ]
                        for _, _r in _grp_sorted.iterrows():
                            _t = pd.to_datetime(_r["draw_datetime"]).strftime("%H:%M")
                            _s = int(_r["samples"])
                            _lines.append(
                                f"{_t} - {_s} sample{'s' if _s != 1 else ''}"
                            )
                        _details[(_tech, int(_hour))] = "<br>".join(_lines)

                _hovertext = [
                    [
                        _details.get(
                            (_tech, _h),
                            f"<b>{_tech} @ {HOUR_LABELS[_h]}</b>"
                            f"<br>No draws this hour",
                        )
                        for _h in _hours_subset
                    ]
                    for _tech in _techs
                ]

            # Custom colorscale: z=0 renders pure white so empty cells
            # don't show a coloured tint (now that we no longer NaN-
            # mask). For z > 0 we use the FULL 9-stop ColorBrewer
            # YlOrBr palette so the gradient matches the original
            # `colorscale="YlOrBr"` look — a 3-stop white→cream→brown
            # shortcut (used in v3 first cut) compressed every mid-
            # range cell into a flat blend and changed the visual
            # appearance noticeably. These hex codes are the
            # ColorBrewer YlOrBr-9 sequential ramp.
            _pa_colorscale = [
                [0.0,     "#ffffff"],   # z = 0 → pure white
                [0.0001,  "#ffffe5"],   # YlOrBr stop 0  (palest)
                [0.125,   "#fff7bc"],   # YlOrBr stop 1
                [0.25,    "#fee391"],   # YlOrBr stop 2
                [0.375,   "#fec44f"],   # YlOrBr stop 3
                [0.5,     "#fe9929"],   # YlOrBr stop 4
                [0.625,   "#ec7014"],   # YlOrBr stop 5
                [0.75,    "#cc4c02"],   # YlOrBr stop 6
                [0.875,   "#993404"],   # YlOrBr stop 7
                [1.0,     "#662506"],   # YlOrBr stop 8  (darkest)
            ]

            # Heatmap renders the colored cells with `hoverinfo='skip'`
            # so the heatmap trace contributes NOTHING to plotly's hover
            # pipeline. All previous fixes failed because plotly's
            # heatmap hover (src/traces/heatmap/hover.js) uses a
            # `findBin` hit-test that misbehaves on short charts — and
            # tweaking heatmap config alone has not worked. v5 routes
            # AROUND the buggy code path entirely by layering a scatter
            # trace on top that carries the actual hover events.
            # Scatter's hover (src/traces/scatter/hover.js) uses
            # Euclidean distance from each marker to the cursor — a
            # completely separate algorithm — and is known to be
            # reliable on short charts where heatmap hover is not.
            _heatmap_kwargs = dict(
                z=_z,
                x=_x,
                y=_techs,
                text=_text_vals,
                texttemplate="%{text}",
                hoverinfo="skip",  # No hover from heatmap — scatter handles it
                colorscale=_pa_colorscale,
                zmin=0,
                zmax=_vmax_pa,
                xgap=1,
                ygap=1,
                showscale=False,
                # zsmooth=False disables rendering interpolation, which
                # otherwise can amplify sub-pixel hit-test errors on
                # short charts.
                zsmooth=False,
            )

            # Scatter overlay — one invisible marker per cell, sized to
            # cover the cell so hovering anywhere inside it hits the
            # marker. Plotly's scatter hover uses Euclidean distance
            # (distance from cursor to marker centre, minus marker
            # radius), then compares against the layout `hoverdistance`.
            # With marker_size=50 (radius 25) and the default
            # `hoverdistance=20`, hover fires within 45 px of any
            # marker centre — easily covering the ~50 px-wide × 28 px-
            # tall cells. Adjacent markers overlap but `hovermode=
            # "closest"` picks the nearest centre, so each cursor
            # position resolves to exactly one cell.
            _scatter_x = []
            _scatter_y = []
            _scatter_hover = []
            for _i, _tech in enumerate(_techs):
                for _j in range(len(_hours_subset)):
                    _scatter_x.append(_x[_j])
                    _scatter_y.append(_tech)
                    _scatter_hover.append(_hovertext[_i][_j])

            _fig = _pgo.Figure()
            _fig.add_trace(_pgo.Heatmap(**_heatmap_kwargs))
            _fig.add_trace(_pgo.Scatter(
                x=_scatter_x,
                y=_scatter_y,
                mode="markers",
                marker=dict(
                    size=50,
                    color="rgba(0,0,0,0)",
                    line=dict(width=0),
                    opacity=0,
                ),
                hovertext=_scatter_hover,
                hoverinfo="text",
                showlegend=False,
                name="",
            ))

            # Row-driven chart height — TARGET: each cell is exactly
            # 28 px tall, regardless of how many tech rows the section
            # has. Achieved by `total_height = n * 28 + chrome` where
            # chrome is the EXACT vertical space Plotly subtracts from
            # `layout.height` to render the surrounding axes.
            #
            # Chrome breakdown (verified against plotly.js source —
            # `src/plots/plots.js::initMargins()` does
            # `plot_h = layout.height - margin.t - margin.b`):
            #   • margin.t = 10
            #   • margin.b = 30  (enough for the 10-pt x-axis tick
            #     labels + descender + small padding; previously was
            #     10 which forced labels to render outside the
            #     reserved margin — invisible/clipped — but the chrome
            #     SUBTRACTED from layout.height stayed at 20 px,
            #     causing the prior `+100` budget to LEAK 80 px into
            #     the plot area; those 80 px got distributed across
            #     cells, producing `28 + 80/n` per cell — i.e. 68 px
            #     in a 2-row chart vs 37 px in a 9-row chart, the
            #     ~2× ratio the user reported)
            #   • Total chrome = 40 px
            #
            # automargin=True is on the y-axis only — Plotly's
            # MARGIN_MAPPING.height ties t/b to x-axes (NOT y), so
            # yaxis automargin only affects L/R margins. The x-axis
            # has no automargin, so chrome is constant at exactly
            # 40 px regardless of label content.
            #
            # Per-cell after this change:  (n*28 + 40 - 40) / n = 28
            # for ALL n. Uniform.
            _plot_h = len(_techs) * 28 + 40
            _fig.update_layout(
                height=_plot_h,
                margin=dict(l=10, r=10, t=10, b=30),
                paper_bgcolor="rgba(0,0,0,0)",
                plot_bgcolor="rgba(0,0,0,0)",
                dragmode=False,
                # Hover hit-test tuning. `hovermode="closest"` is the
                # plotly default but pin it explicitly against future
                # Streamlit / template overrides. `spikedistance=-1`
                # disables spike-line competition entirely (otherwise on
                # short charts a spike point can outrank the heatmap as
                # the "winning" hover target — see plotly.js
                # src/traces/heatmap/hover.js:117-121). Leave
                # `hoverdistance` at its default 20 px; v2 tried
                # `hoverdistance=1` which was too aggressive — restricted
                # heatmap hover to a 2-pixel window around each cell
                # center, breaking hover everywhere, not just on short
                # charts.
                hovermode="closest",
                spikedistance=-1,
                xaxis=dict(
                    tickfont=dict(size=10), side="bottom", fixedrange=True,
                ),
                yaxis=dict(
                    tickfont=dict(size=10), autorange="reversed",
                    fixedrange=True, automargin=True,
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
                    # responsive=True triggers Plotly.Plots.resize() on
                    # window/container resize so internal pixel ↔ data
                    # coordinate mappings stay in sync with the rendered
                    # SVG. Streamlit's iframe occasionally produces a
                    # stale mapping after resize, which can drift the
                    # hover hit-test by a pixel or two on short charts.
                    "responsive": True,
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

        for _pa_shift in _PA_SHIFT_ORDER.get(pa_location, [None]):
            if pa_location != "HC3" and _pa_shift is not None:
                # Per-shift heading uses the shared .section-heading class
                # (16 px / weight 500 / cardinal-stripe gold underline) for
                # visual parity with the analytics dashboard's section
                # headings. Previously used `st.subheader` which renders an
                # <h3> at Streamlit's default ~28 px — dominated the page
                # visually and looked inconsistent with the rest of the app.
                st.markdown(
                    f'<div class="section-heading">{pa_location} - {_pa_shift}</div>',
                    unsafe_allow_html=True,
                )
            _hkey = f"heatmap_{pa_location}_{_pa_shift or 'all'}"
            _render_pa_heatmap(
                _draw_df, pa_location, _pa_shift, pa_view, _hkey,
                (pa_h_start, pa_h_end),
            )

    except Exception as e:
        st.exception(e)
