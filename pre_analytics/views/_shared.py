"""
pre_analytics/views/_shared.py — Module-level chart helpers and
constants for the pre-analytics dashboard.

In Batch 5 Phase 3, the three chart-rendering helpers
(render_pa_subplot_heatmaps, render_pa_hourly_bar,
render_pa_weekday_pattern) were lifted out of `render()` in
pre_analytics/dashboard.py — previously they were nested inside
the render function's try block, which made them un-importable
and hard to test or cache. The constants PA_SHIFT_ORDER and the
shared plotly / numpy aliases are also here so the dashboard
dispatcher stays slim.

Naming change: the leading-underscore variants from the old
nested definitions became public names because they're now
imported across modules.
"""

import calendar as _calp

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import plotly.subplots as sp
import streamlit as st

from config import HOUR_LABELS
from pre_analytics.data import build_draw_pivot, build_draw_count_pivot


# Shift orderings per pre-analytics location. PMOB and HC3 have no
# shift assignments, so they get a single None entry which the
# helpers treat as "all techs for this location, no shift filter".
PA_SHIFT_ORDER: dict[str, list] = {
    "Keck":   ["Early AM", "AM", "PM", "NS"],
    "Norris": ["AM", "PM", "NS"],
    "HC3":    [None],
    "PMOB":   [None],
}


def render_pa_subplot_heatmaps(draw_df, location, view, hour_range,
                               year, month, key):
    """Render all shifts for a location as a single subplots figure.

    Each shift gets its own subplot, vertically stacked, with the
    shift name as a Plotly subplot title (USC maroon, left-aligned).
    A right-side "Total" column on each subplot shows each tech's
    total draws (or per-day average in Monthly view); hover on the
    Total cell surfaces both total draws AND total samples.

    Each subplot title carries hover-only metadata (active techs,
    total draws, total samples for the shift). Hover fires through
    Plotly's annotation hover (`captureevents` + `hovertext`).

    Subplots share `_vmax` (95th-percentile of non-zero values
    across ALL shifts) so the YlOrBr gradient is consistent
    across the location — a "5" cell means the same draw count
    on every shift.

    Sizing: every subplot's plot area is exactly `n_techs * 28`
    px, giving uniform 28-px cells across shifts regardless of
    tech count. Implemented by overriding `yaxis.domain` per
    subplot in paper coords computed from absolute pixel targets
    (the native way to pin absolute plot-area heights — Plotly's
    `row_heights` is relative AND includes chrome, so it can't
    give exact cell heights).

    Caveat: shifts with 1-2 techs have plot areas below plotly's
    findBin hover-precision threshold (~120 px), so cell tooltips
    may be flaky on those — trade-off for uniform cell sizing.
    """
    _h_start, _h_end = hour_range
    _hours_subset = list(range(_h_start, _h_end + 1))
    _n_hours = len(_hours_subset)

    _shifts = PA_SHIFT_ORDER.get(location, [None])

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
        _calp.monthrange(year, month)[1] if view == "Monthly" else 1
    )

    # Pass 1 — build per-shift bookkeeping (pivot, techs, summary).
    #
    # Monthly view uses PER-TECH ACTIVE DAYS as the denominator
    # (not calendar days), so each tech's averages reflect their
    # own working pattern. A tech who worked 10 days has cells
    # showing draws/day across those 10 days, not diluted by 21
    # days they didn't work. `build_draw_count_pivot` returns
    # raw counts + a per-tech active-day dict; we divide row by
    # row. Daily view keeps the simpler `build_draw_pivot` path
    # since N=1 and active-day math is moot.
    _shift_data = []
    for _shift in _shifts:
        if view == "Monthly":
            # Hour-range filter happens AFTER counts/active-days
            # are computed at the (location, shift) level — but
            # we want per-tech N to reflect days the tech was
            # active IN THE HOUR RANGE. So build the count
            # pivot from the hour-filtered draw_df slice.
            if _shift is None:
                _hr_slice = _loc_hr_df
            else:
                _hr_slice = (
                    _loc_hr_df[_loc_hr_df["shift"] == _shift]
                    if not _loc_hr_df.empty else _loc_hr_df
                )
            _counts_pv, _tech_active_days = build_draw_count_pivot(
                _hr_slice if not _hr_slice.empty else draw_df,
                location, _shift,
            )
            _counts_pv = _counts_pv[_hours_subset]

            # Companion samples-sum pivot for the per-cell
            # hover (Monthly view surfaces avg samples in
            # addition to avg draws). Indexed to match
            # _counts_pv so per-tech division aligns.
            if not _hr_slice.empty:
                _samples_sum_pv = _hr_slice.pivot_table(
                    index="display_name", columns="hour",
                    values="samples", aggfunc="sum",
                    fill_value=0,
                ).reindex(
                    index=_counts_pv.index,
                    columns=_hours_subset,
                    fill_value=0,
                )
            else:
                _samples_sum_pv = _counts_pv * 0

            # Avg = count / active_days. Techs with 0 active
            # days stay at 0 (no division), so they render as
            # empty rows — same as today.
            _pivot = _counts_pv.astype(float).copy()
            _samples_avg_pv = _samples_sum_pv.astype(float).copy()
            for _t in _pivot.index:
                _ad = _tech_active_days.get(_t, 0)
                if _ad > 0:
                    _pivot.loc[_t] = _pivot.loc[_t] / _ad
                    _samples_avg_pv.loc[_t] = (
                        _samples_avg_pv.loc[_t] / _ad
                    )
        else:
            _pivot = build_draw_pivot(
                draw_df, location, _shift, view,
                year=year, month=month,
            )
            _pivot = _pivot[_hours_subset]
            _tech_active_days = {}
            _samples_avg_pv = None

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
            "shift":         _shift,
            "pivot":         _pivot,
            "techs":         _techs,
            "n_techs":       len(_techs),
            "shift_df":      _shift_df,
            "n_draws":       len(_shift_df),
            "n_samples": (
                int(_shift_df["samples"].sum())
                if not _shift_df.empty else 0
            ),
            "active_days":   _tech_active_days,
            "samples_avg":   _samples_avg_pv,
        })

    if not _shift_data:
        st.info("No data found.")
        return

    # Global zmax for consistent YlOrBr gradient across subplots.
    _all_nonzero = [
        v for _d in _shift_data
        for row in _d["pivot"].values
        for v in row
        if v > 0
    ]
    _vmax = (
        float(np.percentile(_all_nonzero, 95))
        if _all_nonzero else 1.0
    )
    _vmax = max(_vmax, 1.0)

    # Row weights are NOT used — we override yaxis.domain per
    # subplot below to get absolute pixel control over plot-area
    # heights, which `row_heights` (relative + includes chrome)
    # can't give us.
    _n_rows = len(_shift_data)

    # Subplot titles — None shift (HC3) renders as empty string,
    # so no title appears for it.
    _titles = [
        _d["shift"] if _d["shift"] else "" for _d in _shift_data
    ]

    # X-axis tick layout — hour labels + single Total column label.
    # Numeric x-coords (0..n-1) avoid the categorical heatmap
    # half-width rendering bug; tick labels are mapped back via
    # tickvals/ticktext.
    _x_hour_coords = list(range(_n_hours))
    _x_total_coord = _n_hours
    _tick_vals = _x_hour_coords + [_x_total_coord]
    _tick_text = (
        [HOUR_LABELS[h] for h in _hours_subset] + ["Total"]
    )

    _fig = sp.make_subplots(
        rows=_n_rows, cols=1,
        shared_xaxes=False,   # hour labels show on every subplot
        vertical_spacing=0.02,  # ignored (domains overridden below)
        subplot_titles=_titles,
    )

    # Pass 2 — add hours + totals traces per shift.
    for _i, _d in enumerate(_shift_data, start=1):
        _z_arr = _d["pivot"].values.astype(float)
        _z_masked = np.where(_z_arr == 0, np.nan, _z_arr).tolist()
        # Cell text: integer counts for Daily (exact values),
        # one decimal for Monthly (per-tech-active-day averages
        # are typically fractional and rounding loses signal).
        if view == "Monthly":
            _text_hours = [
                [f"{v:.1f}" if v > 0 else "" for v in row]
                for row in _z_arr
            ]
        else:
            _text_hours = [
                [str(int(round(v))) if v > 0 else "" for v in row]
                for row in _z_arr
            ]

        # Per-cell hover detail (Daily = per-draw breakdown,
        # Monthly = avg with per-tech N).
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
                # itertuples is ~10× faster than iterrows for the inner
                # per-draw loop and avoids the per-row Series boxing.
                # draw_datetime is already a Timestamp (from the parquet
                # read) so .strftime() works directly — drop the redundant
                # pd.to_datetime() round-trip.
                for _row in _grp_sorted.itertuples(index=False):
                    _t = _row.draw_datetime.strftime("%H:%M")
                    _s = int(_row.samples)
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
                    _sv = (
                        _d["samples_avg"].loc[_tech].iloc[_j]
                        if _d["samples_avg"] is not None else 0.0
                    )
                    _ad = _d["active_days"].get(_tech, 0)
                    if _v > 0:
                        _detail = (
                            f"{_v:.1f} avg draws<br>"
                            f"{_sv:.1f} avg samples<br>"
                            f"N = {_ad} active day"
                            f"{'s' if _ad != 1 else ''}"
                        )
                    else:
                        _detail = "No draws this hour"
                else:
                    _detail = _details.get(
                        (_tech, _h), "No draws this hour"
                    )
                _row_cd.append([_label, _detail])
            _cd_hours.append(_row_cd)

        # Hours trace — YlOrBr gradient, NaN-masked zeros.
        _fig.add_trace(
            go.Heatmap(
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

        # Totals trace — one cell per tech showing total draws,
        # flat neutral grey. Hover surfaces both total draws AND
        # total samples for the tech.
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
                # Per-tech active days as the denominator —
                # honest per-active-day averages, not diluted
                # by calendar days the tech wasn't working.
                _ad = _d["active_days"].get(_tech, 0)
                _denom = max(_ad, 1)
                _d_val = _n_d_tech / _denom
                _s_val = _n_s_tech / _denom
                _cell_txt = (
                    f"{_d_val:.1f}" if _n_d_tech > 0 else ""
                )
                _hover = (
                    f"{_d_val:.1f} avg draws/active day · "
                    f"{_s_val:.1f} avg samples/active day · "
                    f"N = {_ad} active day"
                    f"{'s' if _ad != 1 else ''}"
                )
            else:
                _cell_txt = (
                    str(_n_d_tech) if _n_d_tech > 0 else ""
                )
                _hover = (
                    f"{_n_d_tech:,} total draw"
                    f"{'s' if _n_d_tech != 1 else ''} · "
                    f"{_n_s_tech:,} total sample"
                    f"{'s' if _n_s_tech != 1 else ''}"
                )

            _z_totals.append([0])  # flat colorscale, value irrelevant
            _text_totals.append([_cell_txt])
            _cd_totals.append([["Total", _hover]])

        _fig.add_trace(
            go.Heatmap(
                z=_z_totals,
                x=[_x_total_coord],
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

    # Absolute pixel sizing — every subplot's plot area is
    # exactly `n_techs * 28` px, giving uniform 28-px cells
    # across shifts regardless of how many techs each one has.
    # Implemented by overriding `yaxis.domain` per subplot in
    # paper coords (computed from pixel targets). `row_heights`
    # was rejected because it's relative AND includes chrome,
    # so it can't pin absolute cell heights.
    #
    # Caveat: shifts with 1-2 techs get plot areas under
    # plotly's findBin hover threshold (~120 px); cell tooltips
    # may be flaky on those. Trade-off the user accepted in
    # exchange for uniform cells across the location.
    _PX_PER_TECH    = 28
    _PX_TITLE_H     = 28
    _PX_XAXIS_H     = 30
    _PX_INTER_GAP   = 18
    _PX_MARGIN_T    = 20
    _PX_MARGIN_B    = 20

    _plot_areas = [
        _d["n_techs"] * _PX_PER_TECH for _d in _shift_data
    ]
    _total_h = (
        _PX_MARGIN_T
        + sum(_PX_TITLE_H + p + _PX_XAXIS_H for p in _plot_areas)
        + (_n_rows - 1) * _PX_INTER_GAP
        + _PX_MARGIN_B
    )

    # Compute each subplot's y-axis domain in paper coords.
    # Working top-down from the figure top.
    _y_domains = []
    _y_px_top = _total_h - _PX_MARGIN_T
    for _plot_h in _plot_areas:
        _title_top_px = _y_px_top
        _plot_top_px  = _title_top_px - _PX_TITLE_H
        _plot_bot_px  = _plot_top_px - _plot_h
        _xax_bot_px   = _plot_bot_px - _PX_XAXIS_H

        _y_domains.append([
            _plot_bot_px / _total_h,
            _plot_top_px / _total_h,
        ])

        _y_px_top = _xax_bot_px - _PX_INTER_GAP

    # Override each subplot's y-axis domain. X-axis is anchored
    # to its paired y-axis so x ticks follow automatically.
    for _i in range(_n_rows):
        _yax_key = f"yaxis{_i + 1}" if _i > 0 else "yaxis"
        _fig.layout[_yax_key].domain = _y_domains[_i]

    # Restyle the subplot title annotations + attach per-shift
    # hover (active techs / draws / samples). `make_subplots`
    # creates one annotation per row with `xref="x{i} domain"`,
    # x=0.5, centered above the subplot — restyle to USC maroon
    # bold, left-aligned. Also reposition y to match the
    # overridden y-axis domains (default y is based on the
    # auto-computed domain which we replaced above). NOTE:
    # annotation hoverlabel only accepts
    # bgcolor/bordercolor/font (not `align`).
    for _i, _ann in enumerate(_fig.layout.annotations):
        if _i >= len(_shift_data):
            continue
        _d = _shift_data[_i]
        if _d["shift"] is None:
            continue
        _dom_top = _y_domains[_i][1]
        _ann.update(
            text=f"<b>{_d['shift']}</b>",
            font=dict(
                size=13, color="#6F1828",
                family="Inter, system-ui, sans-serif",
            ),
            x=0,
            xanchor="left",
            y=_dom_top + 4 / _total_h,
            yanchor="bottom",
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

    _fig.update_layout(
        height=_total_h,
        margin=dict(
            l=10, r=10,
            t=_PX_MARGIN_T, b=_PX_MARGIN_B,
        ),
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


def render_pa_hourly_bar(draw_df, location, hour_range,
                          date_label, key):
    """Render the Daily-view hourly draws bar chart.

    X-axis = hours in the selected range, Y-axis = total draws
    for that hour at the selected location. Bars are USC maroon
    (#790A26) — matches the analytics dashboard's hourly chart.
    Hover surfaces total draws, total samples, and the distinct
    active-tech count for each hour.
    """
    _h_start, _h_end = hour_range
    _hours = list(range(_h_start, _h_end + 1))

    _df = (
        draw_df[
            (draw_df["location"] == location)
            & (draw_df["hour"] >= _h_start)
            & (draw_df["hour"] <= _h_end)
        ]
        if not draw_df.empty else draw_df
    )

    st.markdown(
        f'<div class="section-heading">'
        f'Hourly draws · {date_label}'
        f'</div>',
        unsafe_allow_html=True,
    )

    if _df.empty:
        st.info("No data found.")
        return

    # Aggregate per hour: total draws, total samples, distinct techs.
    _agg = _df.groupby("hour").agg(
        draws=("display_name", "count"),
        samples=("samples", "sum"),
        techs=("display_name", "nunique"),
    )
    # Reindex to include every hour in the range (zero-fill hours
    # with no draws) so the bar chart shows the full axis.
    _agg = _agg.reindex(_hours, fill_value=0)

    _x_labels = [HOUR_LABELS[h] for h in _hours]
    _y_draws  = _agg["draws"].tolist()
    _samples  = _agg["samples"].astype(int).tolist()
    _techs    = _agg["techs"].astype(int).tolist()

    _customdata = [
        [int(_samples[_i]), int(_techs[_i])]
        for _i in range(len(_hours))
    ]

    _fig = go.Figure()
    _fig.add_trace(
        go.Bar(
            x=_x_labels,
            y=_y_draws,
            marker_color="#790A26",
            customdata=_customdata,
            hovertemplate=(
                "<b>%{x}</b><br>"
                "%{y:,} total draws<br>"
                "%{customdata[0]:,} total samples<br>"
                "%{customdata[1]} active tech"
                "<extra></extra>"
            ),
        )
    )

    # Dashed reference line at the day's mean hourly volume. Lets
    # the user spot above/below-average hours at a glance without
    # eyeballing every bar height. Computed across hours that had
    # any draws — the all-zero pre-shift hours would deflate the
    # mean if included, making most working hours appear "above
    # average" trivially.
    _nonzero_y = [v for v in _y_draws if v > 0]
    if _nonzero_y:
        _mean_draws = sum(_nonzero_y) / len(_nonzero_y)
        _fig.add_hline(
            y=_mean_draws,
            line_dash="dash",
            line_color="#aaaaaa",
            line_width=1,
            annotation_text=f"avg {_mean_draws:.0f}",
            annotation_position="top right",
            annotation_font=dict(size=10, color="#666666"),
        )
    _fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        dragmode=False,
        xaxis=dict(
            tickfont=dict(size=10),
            title=None,
            fixedrange=True,
            categoryorder="array",
            categoryarray=_x_labels,
        ),
        yaxis=dict(
            tickfont=dict(size=10),
            title="Total draws",
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


def render_pa_weekday_pattern(draw_df, location, hour_range,
                               year, month, month_label, key):
    """Render the Monthly-view day-of-week × hour heatmap.

    VOLUME-FOCUSED chart (not tech productivity). For each
    (weekday, hour) slot, the cell shows the average draws on
    a typical occurrence of that weekday — i.e. total draws /
    number of times that weekday had data in the month. This
    is intentionally NOT a per-tech average; that's what the
    main heatmap is for.

    Layout:
      - Rows = days of week (Mon-Sun) THAT HAVE DATA. Days the
        location never operates on (e.g. HC3 weekends) are
        hidden entirely to declutter.
      - Each row label appends a "(×N)" occurrence count
        showing how many dates of that weekday had data in
        the month — same divisor used for that row's averages.
      - X-axis = hours in the selected range + a right-side
        "Total" column (flat neutral grey, no gradient).
      - Section title shows "N = K data days" where K is the
        total distinct calendar dates with data this month at
        this location — exposes the chart's overall coverage.

    Hover surfaces averages + totals for context. NO tech
    count (this chart is about volume, not staffing).
    """
    _h_start, _h_end = hour_range
    _hours = list(range(_h_start, _h_end + 1))
    _n_hours = len(_hours)

    _weekdays = [
        "Monday", "Tuesday", "Wednesday", "Thursday",
        "Friday", "Saturday", "Sunday",
    ]

    _df = (
        draw_df[
            (draw_df["location"] == location)
            & (draw_df["hour"] >= _h_start)
            & (draw_df["hour"] <= _h_end)
        ].copy()
        if not draw_df.empty else draw_df.copy()
    )

    if _df.empty:
        st.markdown(
            f'<div class="section-heading">'
            f'Day-of-week pattern · {month_label}'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.info(
            f"No draws to chart for **{location}** in "
            f"**{month_label}**."
        )
        return

    _df["weekday"] = pd.to_datetime(
        _df["draw_datetime"]
    ).dt.day_name()
    _df["date_only"] = pd.to_datetime(
        _df["draw_datetime"]
    ).dt.date

    # ─── Per-(weekday, hour) totals: draws, samples ───────────
    _tot_draws_pv = _df.pivot_table(
        index="weekday", columns="hour",
        values="display_name", aggfunc="count", fill_value=0,
    ).reindex(index=_weekdays, columns=_hours, fill_value=0)

    _tot_samples_pv = _df.pivot_table(
        index="weekday", columns="hour",
        values="samples", aggfunc="sum", fill_value=0,
    ).reindex(index=_weekdays, columns=_hours, fill_value=0)

    # ─── Per-weekday DATA occurrences ─────────────────────────
    # Count of distinct calendar dates with data per weekday.
    # Used both as the per-row divisor (e.g. 4 Mondays with
    # data → divide Monday row by 4) and to filter out rows
    # with no data (HC3 weekends, etc.).
    _data_occ = (
        _df.groupby("weekday")["date_only"]
        .nunique()
        .reindex(_weekdays, fill_value=0)
        .astype(int)
        .to_dict()
    )

    # Visible weekdays = those with at least one data day.
    _visible_wd = [wd for wd in _weekdays if _data_occ[wd] > 0]
    if not _visible_wd:
        st.markdown(
            f'<div class="section-heading">'
            f'Day-of-week pattern · {month_label}'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.info(
            f"No draws to chart for **{location}** in "
            f"**{month_label}**."
        )
        return

    # Total distinct data days this month (for the section
    # heading's "N = X data days"). Sum of per-weekday data
    # occurrences == count of distinct date_only values, so
    # either computation works; using nunique for clarity.
    _n_data_days = int(_df["date_only"].nunique())

    # ─── Section heading + legend (now that we know N) ────────
    st.markdown(
        f'<div class="section-heading">'
        f'Day-of-week pattern · {month_label} · '
        f'N = {_n_data_days} data day'
        f'{"s" if _n_data_days != 1 else ""}'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        '<div class="heatmap-legend">'
        'Values = avg draws per day in hour. '
        'Colour scale: &nbsp;'
        '<strong style="color:#fff7bc;">■</strong> low &nbsp;→&nbsp; '
        '<strong style="color:#8c2d04;">■</strong> high'
        '</div>',
        unsafe_allow_html=True,
    )

    # Y-labels include the per-row data-occurrence count.
    _y_labels = [
        f"{wd} (×{_data_occ[wd]})" for wd in _visible_wd
    ]

    # ─── Convert totals → per-occurrence averages ─────────────
    # Hour-cell avg = total / data_occurrences_for_that_weekday
    _avg_draws_pv   = _tot_draws_pv.astype(float).copy()
    _avg_samples_pv = _tot_samples_pv.astype(float).copy()
    for _wd in _weekdays:
        _occ = _data_occ[_wd]
        if _occ > 0:
            _avg_draws_pv.loc[_wd]   /= _occ
            _avg_samples_pv.loc[_wd] /= _occ

    # Per-weekday daily totals (Total column) = sum across
    # hours / data_occurrences. Same divisor as the cells.
    _tot_draws_wd_series   = _tot_draws_pv.sum(axis=1)
    _tot_samples_wd_series = _tot_samples_pv.sum(axis=1)
    _avg_draws_wd   = {}
    _avg_samples_wd = {}
    _tot_draws_wd   = {}
    _tot_samples_wd = {}
    for _wd in _visible_wd:
        _occ = max(_data_occ[_wd], 1)
        _avg_draws_wd[_wd]   = _tot_draws_wd_series[_wd]   / _occ
        _avg_samples_wd[_wd] = _tot_samples_wd_series[_wd] / _occ
        _tot_draws_wd[_wd]   = int(_tot_draws_wd_series[_wd])
        _tot_samples_wd[_wd] = int(_tot_samples_wd_series[_wd])

    # ─── Hour cells trace (visible rows only) ─────────────────
    _avg_draws_visible = _avg_draws_pv.loc[_visible_wd]
    _z_arr = _avg_draws_visible.values
    _flat_nonzero = [v for row in _z_arr for v in row if v > 0]
    _vmax_hours = (
        float(np.percentile(_flat_nonzero, 95))
        if _flat_nonzero else 1.0
    )
    _vmax_hours = max(_vmax_hours, 1.0)

    _z_masked = np.where(_z_arr == 0, np.nan, _z_arr).tolist()
    _text_cells = [
        [f"{v:.1f}" if v > 0 else "" for v in row]
        for row in _z_arr
    ]

    # customdata per hour cell: [hour_label, avg_draws,
    # avg_samples, total_draws, total_samples]. Hover surfaces
    # both averages AND the underlying totals for transparency.
    _cd_hours = []
    for _wd in _visible_wd:
        _row_cd = []
        for _j, _h in enumerate(_hours):
            _label = HOUR_LABELS[_h]
            _av_d = float(_avg_draws_pv.loc[_wd].iloc[_j])
            _av_s = float(_avg_samples_pv.loc[_wd].iloc[_j])
            _t_d  = int(_tot_draws_pv.loc[_wd].iloc[_j])
            _t_s  = int(_tot_samples_pv.loc[_wd].iloc[_j])
            _row_cd.append([_label, _av_d, _av_s, _t_d, _t_s])
        _cd_hours.append(_row_cd)

    _fig = go.Figure()
    _fig.add_trace(
        go.Heatmap(
            z=_z_masked,
            x=list(range(_n_hours)),
            y=_y_labels,
            text=_text_cells,
            texttemplate="%{text}",
            hoverinfo="text",
            colorscale="YlOrBr",
            zmin=0,
            zmax=_vmax_hours,
            xgap=1,
            ygap=1,
            showscale=False,
            customdata=_cd_hours,
            hovertemplate=(
                "<b>%{y} @ %{customdata[0]}</b><br>"
                "%{customdata[1]:.1f} avg draws<br>"
                "%{customdata[2]:.1f} avg samples"
                "<extra></extra>"
            ),
            hoverongaps=False,
        )
    )

    # ─── Total column trace ───────────────────────────────────
    # Flat neutral grey — daily-total magnitudes aren't on the
    # same scale as the per-hour averages, so a gradient here
    # would mislead users into apples-to-oranges comparisons.
    _x_total_coord = _n_hours
    _z_totals = [[0] for _ in _visible_wd]
    _text_totals = [
        [f"<b>{_avg_draws_wd[_wd]:.0f}</b>"
         if _avg_draws_wd[_wd] > 0 else ""]
        for _wd in _visible_wd
    ]
    _cd_totals = [
        [["Total",
          _avg_draws_wd[_wd],
          _avg_samples_wd[_wd],
          _tot_draws_wd[_wd],
          _tot_samples_wd[_wd]]]
        for _wd in _visible_wd
    ]

    _fig.add_trace(
        go.Heatmap(
            z=_z_totals,
            x=[_x_total_coord],
            y=_y_labels,
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
                "%{customdata[1]:.1f} avg daily total draws<br>"
                "%{customdata[2]:.1f} avg daily total samples"
                "<extra></extra>"
            ),
            textfont=dict(color="#1a1a1a", size=11),
        )
    )

    _fig.update_layout(
        height=len(_visible_wd) * 32 + 50,
        margin=dict(l=10, r=10, t=10, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        dragmode=False,
        xaxis=dict(
            tickmode="array",
            tickvals=list(range(_n_hours)) + [_x_total_coord],
            ticktext=(
                [HOUR_LABELS[h] for h in _hours] + ["Total"]
            ),
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
        key=key,
        config={
            "staticPlot": False,
            "scrollZoom": False,
            "displayModeBar": False,
        },
    )
