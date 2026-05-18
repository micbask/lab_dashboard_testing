"""
analytics/views/tat.py — TAT (turnaround time) analytics view.

Renders, in order:
  1. Priority legend (coloured dot + abbreviation + target).
  2. Summary by priority — Plotly go.Table (RT / ST / TS / All).
  3. Procedure filter (st.multiselect, defaults to top 5 or core panel).
  4. Turnaround time by procedure — Plotly go.Table (split into a
     frozen Procedure column + scrollable stats columns).
  5. Mean TAT by procedure — horizontal grouped bar chart with
     dashed reference lines at each priority's target.

Extracted from analytics/dashboard.py in Batch 5 Phase 2.
"""

import calendar as _cal
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import BENCHES_USING_CORE_PANEL, CORE_PANEL_DEFAULTS
from storage import get_index_hash
from ui_components import render_header
from formatting import format_tat, format_pct, format_range
from analytics.data import (
    load_analytics_data, compute_tat_metrics, build_tat_table,
    get_top_procedures_by_volume, get_tat_targets,
)
from analytics.views._shared import (
    TAT_ROUTINE_COLOR, TAT_STAT_COLOR, TAT_TS_COLOR, TAT_COMBINED_COLOR,
)


def render_tat_view(params: dict) -> None:
    """Render the TAT analytics page (see module docstring for layout)."""
    _bench     = params.get("tat_bench") or params["map_type"]
    _date_str  = params.get("tat_date_str") or ""
    _view      = params["view_mode"]
    _resources = tuple(params.get("current_resources") or ())

    if _view == "Daily" and _date_str:
        try:
            _date_label = pd.Timestamp(_date_str).strftime("%B %d, %Y")
        except Exception:
            _date_label = _date_str
    elif _view == "Monthly" and len(_date_str) >= 7:
        try:
            _yr, _mo = int(_date_str[:4]), int(_date_str[5:7])
            _date_label = f"{_cal.month_name[_mo]} {_yr}"
        except Exception:
            _date_label = _date_str
    else:
        _date_label = _date_str or "-"

    render_header(_bench, f"{_date_label} · TAT")

    # ── Priority legend ────────────────────────────────────────────────────
    # Colored dot + abbreviation + target, one row under the dashboard
    # subtitle. Targets are resolved via get_tat_targets(bench) — most
    # benches use the default RT=2h / ST=TS=1h; Norris Specialty
    # overrides all three to 48h (send-out SLA). Every downstream
    # consumer (summary stats, headers, bar chart, build_tat_table)
    # reads from the same `_tat_targets` dict so changing the
    # override map updates everything together.
    _tat_targets = get_tat_targets(_bench)
    _rt_target_h = _tat_targets["RT"] // 60
    _st_target_h = _tat_targets["ST"] // 60
    _ts_target_h = _tat_targets["TS"] // 60

    def _legend_chip(color: str, label: str) -> str:
        return (
            '<span style="display:inline-flex;align-items:center;">'
            f'<span style="display:inline-block;width:10px;height:10px;'
            f'border-radius:50%;background:{color};margin-right:6px;"></span>'
            f'{label}</span>'
        )

    st.markdown(
        '<div style="display:flex;flex-wrap:wrap;gap:18px;align-items:center;'
        'font-size:12px;color:rgba(0,0,0,0.6);margin-bottom:16px;">'
        f'{_legend_chip(TAT_ROUTINE_COLOR, f"RT (Routine, target ≤ {_rt_target_h}h)")}'
        f'{_legend_chip(TAT_STAT_COLOR,    f"ST (Stat, target ≤ {_st_target_h}h)")}'
        f'{_legend_chip(TAT_TS_COLOR,      f"TS (Time Study, target ≤ {_ts_target_h}h)")}'
        '</div>',
        unsafe_allow_html=True,
    )

    if not _date_str:
        st.warning("No date string supplied - cannot load TAT data.")
        return

    # Translate date_str + view → (start_date, end_date) for the shared
    # loader. Daily: a single date; Monthly: first..last day of month.
    if _view == "Daily":
        _start_d = date.fromisoformat(_date_str)
        _end_d   = _start_d
    else:
        _yr2 = int(_date_str[:4])
        _mo2 = int(_date_str[5:7])
        _start_d = date(_yr2, _mo2, 1)
        _end_d   = date(_yr2, _mo2, _cal.monthrange(_yr2, _mo2)[1])

    _raw_tat_df = load_analytics_data(
        start_date=_start_d,
        end_date=_end_d,
        resources=_resources,
        time_basis="TAT",
        index_hash=get_index_hash(),
    )
    tat_df = compute_tat_metrics(_raw_tat_df)

    if tat_df.empty:
        st.warning(
            f"No TAT data found for **{_bench}** on **{_date_label}**."
        )
        return

    # ── Summary by priority (Plotly go.Table) ─────────────────────────────
    st.markdown(
        '<div class="section-heading">Summary by priority</div>',
        unsafe_allow_html=True,
    )

    _priorities_order = ["RT", "ST", "TS", "All"]
    _priority_colors_map = {
        "RT":  TAT_ROUTINE_COLOR,
        "ST":  TAT_STAT_COLOR,
        "TS":  TAT_TS_COLOR,
        "All": TAT_COMBINED_COLOR,
    }
    _priority_fills_anchor = {
        "RT":  "rgba(0, 102, 204, 0.18)",
        "ST":  "rgba(204, 102, 0, 0.18)",
        "TS":  "rgba(10, 147, 150, 0.18)",
        "All": "rgba(68, 68, 68, 0.18)",
    }
    _priority_fills_row = {
        "RT":  "rgba(0, 102, 204, 0.08)",
        "ST":  "rgba(204, 102, 0, 0.08)",
        "TS":  "rgba(10, 147, 150, 0.08)",
        "All": "rgba(68, 68, 68, 0.08)",
    }
    _priority_target_labels = {
        "RT":  f"≤ {_rt_target_h}h",
        "ST":  f"≤ {_st_target_h}h",
        "TS":  f"≤ {_ts_target_h}h",
        "All": "-",
    }

    def _fmt_n_table(v) -> str:
        if v is None or pd.isna(v):
            return "-"
        return f"{int(v):,}"

    _summary_rows: list[tuple] = []
    for _prio in _priorities_order:
        if _prio == "All":
            _subset = tat_df
        else:
            _subset = tat_df[tat_df["Collection Priority"] == _prio]
        if _subset.empty:
            _summary_rows.append((_prio, None, None, None, None, None))
            continue
        _tats = _subset["TAT_minutes"].astype(float)
        _n = int(len(_subset))
        _mean = float(_tats.mean())
        _mn = float(_tats.min())
        _mx = float(_tats.max())
        if _prio == "All":
            _known_mask = _subset["Collection Priority"].isin(_tat_targets)
            if _known_mask.any():
                _known_tats = _tats[_known_mask]
                _thresholds = _subset.loc[
                    _known_mask, "Collection Priority"
                ].map(_tat_targets).astype(float)
                _meets = int((_known_tats <= _thresholds).sum())
                _pct = float(_meets / int(_known_mask.sum()) * 100.0)
            else:
                _pct = None
        else:
            _threshold = _tat_targets[_prio]
            _pct = float((_tats <= _threshold).mean() * 100.0)
        _summary_rows.append((_prio, _n, _mean, _pct, _mn, _mx))

    _summary_priority_col = [f"<b>{r[0]}</b>" for r in _summary_rows]
    _summary_n_col     = [_fmt_n_table(r[1]) for r in _summary_rows]
    _summary_mean_col  = [format_tat(r[2])   for r in _summary_rows]
    _summary_targ_col  = [_priority_target_labels[r[0]] for r in _summary_rows]
    _summary_pct_col   = [format_pct(r[3])   for r in _summary_rows]
    _summary_range_col = [
        format_range(r[4], r[5]) for r in _summary_rows
    ]

    _per_row_anchor = [_priority_fills_anchor[r[0]] for r in _summary_rows]
    _per_row_light  = [_priority_fills_row[r[0]]    for r in _summary_rows]
    _summary_cell_fills = [
        _per_row_anchor, _per_row_light, _per_row_light,
        _per_row_light,  _per_row_light, _per_row_light,
    ]

    _summary_priority_text_colors = [
        _priority_colors_map[r[0]] for r in _summary_rows
    ]
    _summary_default_text_colors = ["#1a1a1a"] * len(_summary_rows)
    _summary_font_colors = [
        _summary_priority_text_colors,
        _summary_default_text_colors,
        _summary_default_text_colors,
        _summary_default_text_colors,
        _summary_default_text_colors,
        _summary_default_text_colors,
    ]

    _HEADER_H = 36
    _ROW_H    = 36
    _summary_fig = go.Figure(
        data=go.Table(
            columnwidth=[1.0, 1.0, 1.0, 1.0, 1.0, 1.0],
            header=dict(
                # "% within target" promoted to position 3 (right
                # after n) — it's the headline compliance KPI for
                # this table; Mean TAT / Target / Range are
                # supporting context. Previous order buried the KPI
                # in column 5.
                values=[
                    "Priority", "n", "% within target",
                    "Mean TAT", "Target", "Range",
                ],
                fill_color="#ffffff",
                line_color="#e2e8f0",
                align="center",
                font=dict(
                    family="Inter, system-ui, sans-serif",
                    size=12,
                    color="#6F1828",
                ),
                height=_HEADER_H,
            ),
            cells=dict(
                values=[
                    _summary_priority_col,
                    _summary_n_col,
                    _summary_pct_col,
                    _summary_mean_col,
                    _summary_targ_col,
                    _summary_range_col,
                ],
                fill_color=_summary_cell_fills,
                line_color="#eef0f3",
                align="center",
                font=dict(
                    family="Inter, system-ui, sans-serif",
                    size=13,
                    color=_summary_font_colors,
                ),
                height=_ROW_H,
            ),
        )
    )
    _summary_total_h = _HEADER_H + len(_summary_rows) * _ROW_H + 8
    _summary_fig.update_layout(
        height=_summary_total_h,
        margin=dict(l=4, r=4, t=4, b=4),
        paper_bgcolor="rgba(0,0,0,0)",
        # Let the chart's width track its iframe container so it
        # shrinks with the viewport instead of overflowing and being
        # clipped (the iframe runs with scrolling=False below).
        autosize=True,
    )
    import plotly.io as _pio_summary
    import streamlit.components.v1 as _components_summary
    _summary_html = _pio_summary.to_html(
        _summary_fig,
        include_plotlyjs="cdn",
        full_html=False,
        # responsive=True enables plotly.js's window-resize listener
        # so the table re-lays out when the user narrows the browser.
        # Without it, the chart stays at its default-computed width
        # and the right-most columns ("% within target", "Range")
        # get clipped on narrow viewports.
        config={"displayModeBar": False, "responsive": True},
    )
    _components_summary.html(
        _summary_html,
        height=_summary_total_h + 8,
        scrolling=False,
    )

    # ── Procedure filter ───────────────────────────────────────────────────
    _all_procs = sorted(tat_df["Order Procedure"].dropna().unique().tolist())
    if _bench in BENCHES_USING_CORE_PANEL:
        _present = set(_all_procs)
        _default_top = [p for p in CORE_PANEL_DEFAULTS if p in _present]
        if not _default_top:
            _default_top = get_top_procedures_by_volume(tat_df, n=5)
        _default_label = "core panels"
    else:
        _default_top = get_top_procedures_by_volume(tat_df, n=5)
        _default_label = "top 5 by volume"

    _filter_key = f"tat_proc_filter_{_bench}_{_date_str}"

    st.markdown(
        '<div class="section-heading">Procedures</div>',
        unsafe_allow_html=True,
    )
    _selected = st.multiselect(
        f"Filter procedures (defaults to {_default_label})",
        options=_all_procs,
        default=_default_top,
        key=_filter_key,
    )
    if not _selected:
        _selected = _default_top

    table_df = build_tat_table(tat_df, _selected, targets=_tat_targets)

    # ── TAT table (Plotly go.Table) ────────────────────────────────────────
    st.markdown(
        '<div class="section-heading">Turnaround time by procedure</div>',
        unsafe_allow_html=True,
    )

    _tat_headers = [
        "Procedure",
        f"<span style='color:{TAT_ROUTINE_COLOR}'><b>RT</b></span><br>n",
        f"<span style='color:{TAT_ROUTINE_COLOR}'><b>RT</b></span><br>Mean",
        f"<span style='color:{TAT_ROUTINE_COLOR}'><b>RT</b></span><br>% ≤{_rt_target_h}h",
        f"<span style='color:{TAT_ROUTINE_COLOR}'><b>RT</b></span><br>Range",
        f"<span style='color:{TAT_STAT_COLOR}'><b>ST</b></span><br>n",
        f"<span style='color:{TAT_STAT_COLOR}'><b>ST</b></span><br>Mean",
        f"<span style='color:{TAT_STAT_COLOR}'><b>ST</b></span><br>% ≤{_st_target_h}h",
        f"<span style='color:{TAT_STAT_COLOR}'><b>ST</b></span><br>Range",
        f"<span style='color:{TAT_TS_COLOR}'><b>TS</b></span><br>n",
        f"<span style='color:{TAT_TS_COLOR}'><b>TS</b></span><br>Mean",
        f"<span style='color:{TAT_TS_COLOR}'><b>TS</b></span><br>% ≤{_ts_target_h}h",
        f"<span style='color:{TAT_TS_COLOR}'><b>TS</b></span><br>Range",
        f"<span style='color:{TAT_COMBINED_COLOR}'><b>All</b></span><br>n",
        f"<span style='color:{TAT_COMBINED_COLOR}'><b>All</b></span><br>Mean",
        f"<span style='color:{TAT_COMBINED_COLOR}'><b>All</b></span><br>%",
        f"<span style='color:{TAT_COMBINED_COLOR}'><b>All</b></span><br>Range",
    ]

    _hdr_fill_cells = (
        ["#ffffff"]
        + ["rgba(0, 102, 204, 0.18)"]   * 4
        + ["rgba(204, 102, 0, 0.18)"]   * 4
        + ["rgba(10, 147, 150, 0.18)"]  * 4
        + ["rgba(68, 68, 68, 0.18)"]    * 4
    )
    _cell_fill_cols = (
        ["#ffffff"]
        + ["rgba(0, 102, 204, 0.08)"]   * 4
        + ["rgba(204, 102, 0, 0.08)"]   * 4
        + ["rgba(10, 147, 150, 0.08)"]  * 4
        + ["rgba(68, 68, 68, 0.08)"]    * 4
    )

    def _fmt_n(v):
        if v is None or pd.isna(v):
            return "-"
        return f"{int(v):,}"

    _proc_col   = table_df[("Procedure", "Procedure")].tolist()
    _rt_n_col   = [_fmt_n(v)     for v in table_df[("RT",  "n")]]
    _rt_mean    = [format_tat(v) for v in table_df[("RT",  "Mean")]]
    _rt_pct     = [format_pct(v) for v in table_df[("RT",  "% within target")]]
    _rt_range   = [
        format_range(mn, mx)
        for mn, mx in zip(table_df[("RT", "Min")], table_df[("RT", "Max")])
    ]
    _st_n_col   = [_fmt_n(v)     for v in table_df[("ST",  "n")]]
    _st_mean    = [format_tat(v) for v in table_df[("ST",  "Mean")]]
    _st_pct     = [format_pct(v) for v in table_df[("ST",  "% within target")]]
    _st_range   = [
        format_range(mn, mx)
        for mn, mx in zip(table_df[("ST", "Min")], table_df[("ST", "Max")])
    ]
    _ts_n_col   = [_fmt_n(v)     for v in table_df[("TS",  "n")]]
    _ts_mean    = [format_tat(v) for v in table_df[("TS",  "Mean")]]
    _ts_pct     = [format_pct(v) for v in table_df[("TS",  "% within target")]]
    _ts_range   = [
        format_range(mn, mx)
        for mn, mx in zip(table_df[("TS", "Min")], table_df[("TS", "Max")])
    ]
    _all_n_col  = [_fmt_n(v)     for v in table_df[("All", "n")]]
    _all_mean   = [format_tat(v) for v in table_df[("All", "Mean")]]
    _all_pct    = [format_pct(v) for v in table_df[("All", "% within target")]]
    _all_range  = [
        format_range(mn, mx)
        for mn, mx in zip(table_df[("All", "Min")], table_df[("All", "Max")])
    ]

    _cell_values = [
        _proc_col,
        _rt_n_col,  _rt_mean,  _rt_pct,  _rt_range,
        _st_n_col,  _st_mean,  _st_pct,  _st_range,
        _ts_n_col,  _ts_mean,  _ts_pct,  _ts_range,
        _all_n_col, _all_mean, _all_pct, _all_range,
    ]
    _aligns = ["left"] + ["right"] * 16
    _header_font_colors = ["#6F1828"] + ["#1a1a1a"] * 16

    _PROC_COL_PX = 220
    _STAT_COL_PX = 110
    _STATS_W     = _STAT_COL_PX * 16 + 20
    _PROC_W      = _PROC_COL_PX + 20

    import math as _math_tat
    _PROC_CHARS_PER_LINE = 31
    _STAT_CHARS_PER_LINE = 16
    _LINE_PX        = 20
    _ROW_PADDING_PX = 12
    _MIN_ROW_PX     = _LINE_PX + _ROW_PADDING_PX

    def _est_lines(text, chars_per_line):
        n = max(1, len(str(text)))
        return _math_tat.ceil(n / chars_per_line)

    _row_line_counts = [
        max(
            _est_lines(_proc_col[i],      _PROC_CHARS_PER_LINE),
            _est_lines(_rt_range[i],      _STAT_CHARS_PER_LINE),
            _est_lines(_st_range[i],      _STAT_CHARS_PER_LINE),
            _est_lines(_ts_range[i],      _STAT_CHARS_PER_LINE),
            _est_lines(_all_range[i],     _STAT_CHARS_PER_LINE),
        )
        for i in range(len(_proc_col))
    ]
    _row_height = max(
        _MIN_ROW_PX,
        max(_row_line_counts, default=1) * _LINE_PX + _ROW_PADDING_PX,
    )

    _n_rows = max(1, len(table_df))
    _table_h = 56 + _n_rows * _row_height + 24

    _proc_fig = go.Figure(
        data=go.Table(
            columnwidth=[1],
            header=dict(
                values=["Procedure"],
                fill_color="#ffffff",
                line_color="#e2e8f0",
                align="center",
                font=dict(
                    family="Inter, system-ui, sans-serif",
                    size=12,
                    color="#6F1828",
                ),
                height=56,
            ),
            cells=dict(
                values=[_proc_col],
                fill_color="#ffffff",
                line_color="#eef0f3",
                align=["left"],
                font=dict(
                    family="Inter, system-ui, sans-serif",
                    size=12,
                    color="#1a1a1a",
                ),
                height=_row_height,
            ),
        )
    )
    _proc_fig.update_layout(
        height=_table_h,
        width=_PROC_W,
        margin=dict(l=4, r=0, t=4, b=4),
        paper_bgcolor="rgba(0,0,0,0)",
    )

    _stats_fig = go.Figure(
        data=go.Table(
            columnwidth=[1] * 16,
            header=dict(
                values=_tat_headers[1:],
                fill_color=_hdr_fill_cells[1:],
                line_color="#e2e8f0",
                align="center",
                font=dict(
                    family="Inter, system-ui, sans-serif",
                    size=12,
                    color=_header_font_colors[1:],
                ),
                height=56,
            ),
            cells=dict(
                values=_cell_values[1:],
                fill_color=_cell_fill_cols[1:],
                line_color="#eef0f3",
                align=_aligns[1:],
                font=dict(
                    family="Inter, system-ui, sans-serif",
                    size=12,
                    color="#1a1a1a",
                ),
                height=_row_height,
            ),
        )
    )
    _stats_fig.update_layout(
        height=_table_h,
        width=_STATS_W,
        margin=dict(l=0, r=4, t=4, b=4),
        paper_bgcolor="rgba(0,0,0,0)",
    )

    import plotly.io as _pio
    import streamlit.components.v1 as _components

    _proc_html = _pio.to_html(
        _proc_fig,
        include_plotlyjs="cdn",
        full_html=False,
        config={"displayModeBar": False},
    )
    _stats_html = _pio.to_html(
        _stats_fig,
        include_plotlyjs=False,
        full_html=False,
        config={"displayModeBar": False},
    )

    _components.html(
        f"""
        <div style="display: flex; width: 100%; align-items: flex-start; padding-bottom: 4px;">
            <div style="flex: 0 0 {_PROC_W}px; min-width: {_PROC_W}px;">
                {_proc_html}
            </div>
            <div style="flex: 1 1 auto; min-width: 0; overflow-x: auto;">
                {_stats_html}
            </div>
        </div>
        """,
        height=_table_h + 24,
        scrolling=False,
    )

    # ── Mean-TAT bar chart ─────────────────────────────────────────────────
    st.markdown(
        '<div class="section-heading">Mean TAT by procedure</div>',
        unsafe_allow_html=True,
    )

    _bar_procs = list(_proc_col)
    _rt_means_raw  = list(table_df[("RT",  "Mean")])
    _st_means_raw  = list(table_df[("ST",  "Mean")])
    _ts_means_raw  = list(table_df[("TS",  "Mean")])
    _all_means_raw = list(table_df[("All", "Mean")])

    def _bar_xs(values):
        return [None if v is None or pd.isna(v) else float(v) for v in values]

    def _bar_hover(values, label):
        return [
            f"<b>{p}</b><br>{label}: {format_tat(v)}<extra></extra>"
            for p, v in zip(_bar_procs, values)
        ]

    _bar_fig = go.Figure()
    _bar_fig.add_trace(go.Bar(
        y=_bar_procs,
        x=_bar_xs(_rt_means_raw),
        name="RT",
        orientation="h",
        marker_color=TAT_ROUTINE_COLOR,
        hovertemplate=_bar_hover(_rt_means_raw, "RT"),
    ))
    _bar_fig.add_trace(go.Bar(
        y=_bar_procs,
        x=_bar_xs(_st_means_raw),
        name="ST",
        orientation="h",
        marker_color=TAT_STAT_COLOR,
        hovertemplate=_bar_hover(_st_means_raw, "ST"),
    ))
    _bar_fig.add_trace(go.Bar(
        y=_bar_procs,
        x=_bar_xs(_ts_means_raw),
        name="TS",
        orientation="h",
        marker_color=TAT_TS_COLOR,
        hovertemplate=_bar_hover(_ts_means_raw, "TS"),
    ))
    _bar_fig.add_trace(go.Bar(
        y=_bar_procs,
        x=_bar_xs(_all_means_raw),
        name="All",
        orientation="h",
        marker_color=TAT_COMBINED_COLOR,
        hovertemplate=_bar_hover(_all_means_raw, "All"),
    ))

    def _fmt_target_label(minutes: int) -> str:
        if minutes >= 60 and minutes % 60 == 0:
            return f"{minutes // 60}h"
        return f"{minutes}m"

    _grouped_targets: dict[int, list[str]] = {}
    for _p in ("RT", "ST", "TS"):
        _grouped_targets.setdefault(_tat_targets[_p], []).append(_p)
    for _t_min, _prios in _grouped_targets.items():
        _bar_fig.add_vline(
            x=_t_min,
            line_dash="dash",
            line_color="#aaaaaa",
            line_width=1,
            annotation_text=f"{'/'.join(_prios)} target ({_fmt_target_label(_t_min)})",
            annotation_position="top",
            annotation_font=dict(size=10, color="#666666"),
        )

    _bar_h = max(320, len(_bar_procs) * 60 + 100)
    _bar_fig.update_layout(
        height=_bar_h,
        barmode="group",
        bargap=0.3,
        bargroupgap=0.1,
        dragmode=False,
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        # Bumped top margin from 50 → 70 px so the legend can sit
        # higher above the plot area without getting clipped — buys
        # the breathing room between the legend chips and the
        # "RT/ST/TS target (Nh)" labels that float at y=1 (plot-area
        # top edge). Sized so the gap is visible but the chart's
        # vertical real estate isn't noticeably reduced.
        margin=dict(l=10, r=10, t=70, b=30),
        legend=dict(
            orientation="h",
            yanchor="bottom", y=1.12,
            xanchor="left",   x=0,
            font=dict(size=12, family="Inter, system-ui, sans-serif"),
        ),
        xaxis=dict(
            title="Mean TAT (minutes)",
            fixedrange=True,
            gridcolor="#eef0f3",
        ),
        yaxis=dict(
            autorange="reversed",
            fixedrange=True,
            tickfont=dict(size=11, family="Inter, system-ui, sans-serif"),
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
        _bar_fig,
        use_container_width=True,
        config={"displayModeBar": False, "scrollZoom": False},
        key="tat_bar_chart",
    )
