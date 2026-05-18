"""
analytics/views/daily.py — Daily heatmap + Hourly Volume bar chart.

Renders for the Completed / In-Lab / Forecast time bases:
  • 4 KPI cards (Total volume, Top procedure, Peak hour, Avg/hour).
  • Procedure × hour-of-day heatmap with Month-avg comparison
    hover tooltips (or Oranges colorscale for forecast).
  • Hourly Volume bar chart.

Extracted from analytics/dashboard.py in Batch 5 Phase 2.
"""

import calendar as _cal
from datetime import date

import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import HOUR_LABELS, LABEL_TO_HOUR
from storage import storage_is_configured
from forecasting import load_forecasts, build_forecast_pivot
from ui_components import metric_card, render_header
from analytics.data import (
    load_analytics_data, build_pivot, load_monthly_avg_for_comparison,
)
from analytics.views._shared import (
    VIRIDIS_LOW, VIRIDIS_HIGH, ORANGES_LOW, ORANGES_HIGH,
    build_analytics_heatmap, render_top_n_legend, apply_local_file_scope,
)


def render_daily_view(params: dict, ss) -> None:
    """Render the Daily heatmap + Hourly Volume bar chart."""
    map_type           = params["map_type"]
    time_basis         = params["time_basis"]
    _local_df          = params["local_df"]
    _current_resources = params["current_resources"]
    _idx_hash          = params["idx_hash"]
    selected_date      = params["selected_date"]
    hour_range         = params["hour_range"]
    _is_forecast_view  = params["is_forecast_date"]

    if _is_forecast_view:
        _fc_panel_data = load_forecasts(map_type)
        if _fc_panel_data is None:
            st.warning(
                f"No forecast data available for **{map_type}**.  "
                "Open Data Management and click **Refresh Forecast** to generate predictions."
            )
            return
        pivot, hours = build_forecast_pivot(
            _fc_panel_data, selected_date, hour_range, time_basis=time_basis,
            top_n=st.session_state.get("analytics_top_n", 10),
            cache_key=f"{map_type}|{_fc_panel_data.get('last_data_date')}",
        )
    else:
        if storage_is_configured():
            filtered_df = load_analytics_data(
                start_date=selected_date,
                end_date=selected_date,
                resources=_current_resources,
                time_basis=time_basis,
                index_hash=_idx_hash,
            )
        else:
            filtered_df = apply_local_file_scope(
                _local_df,
                ss.resource_assignments[map_type],
                time_basis,
            )

        if time_basis == "In-Lab" and filtered_df.empty:
            st.warning("No 'Date/Time - In Lab' data available.")
            return

        pivot, _df_date_hour, _df_date, hours = build_pivot(
            filtered_df, selected_date, hour_range,
            top_n=st.session_state.get("analytics_top_n", 10),
        )

    date_str = pd.Timestamp(selected_date).strftime("%B %d, %Y")
    render_header(map_type, date_str + (" · forecast" if _is_forecast_view else ""))

    if _is_forecast_view:
        st.markdown(
            '<div style="background:#1C1917;color:#e0e0e0;border-left:4px solid #FF9800;'
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
            st.warning("No forecast predictions available.")
        else:
            st.warning("No data found.")
        return

    _hour_cols = [c for c in pivot.columns if c != "Total"]
    if not _hour_cols or pivot.empty:
        st.info("No completed procedures found for this site on the selected date.")
        return

    # KPI math: for the historic (non-forecast) path, compute Total
    # volume / Peak hour / Avg per hour from the BENCH-WIDE filtered
    # DataFrame (not the top-N pivot) so the cards stay stable when
    # the user toggles Top 10/20/30/All. The hour-range slider still
    # scopes the metrics. Top procedure is unchanged — top-of-pivot
    # equals top-of-all by definition.
    #
    # Forecast path keeps the pivot-based math because the forecast
    # data source doesn't expose a bench-wide total in this scope.
    top_proc   = pivot["Total"].idxmax()
    if _is_forecast_view:
        total_vol      = int(round(pivot["Total"].sum()))
        peak_hour      = pivot[_hour_cols].sum().idxmax()
        peak_hour_cnt  = int(round(pivot[_hour_cols].sum()[peak_hour]))
    else:
        _h_start, _h_end = hour_range
        _filt_hr = filtered_df[
            filtered_df["hour"].between(_h_start, _h_end)
        ]
        total_vol = int(_filt_hr["Complete Volume"].fillna(0).sum())
        _per_hour = _filt_hr.groupby("hour")["Complete Volume"].sum()
        if not _per_hour.empty:
            _peak_h_int   = int(_per_hour.idxmax())
            peak_hour     = HOUR_LABELS[_peak_h_int]
            peak_hour_cnt = int(round(float(_per_hour.max())))
        else:
            peak_hour     = pivot[_hour_cols].sum().idxmax()
            peak_hour_cnt = int(round(pivot[_hour_cols].sum()[peak_hour]))
    avg_per_hr = round(total_vol / max(len(_hour_cols), 1), 1)
    _vol_label = "Forecast volume" if _is_forecast_view else "Total volume"

    _m1, _m2, _m3, _m4 = st.columns(4)
    with _m1:
        st.markdown(metric_card(_vol_label, f"{total_vol:,}", accent=True),
                    unsafe_allow_html=True)
    with _m2:
        st.markdown(metric_card("Top procedure", top_proc,
                    sub=f"{int(round(pivot.loc[top_proc, 'Total'])):,} total"),
                    unsafe_allow_html=True)
    with _m3:
        st.markdown(metric_card("Peak hour", peak_hour,
                    sub=f"{peak_hour_cnt} "
                        f"{'predicted' if _is_forecast_view else 'completions'}"),
                    unsafe_allow_html=True)
    with _m4:
        st.markdown(metric_card("Avg / hour", str(avg_per_hr),
                    sub=f"across {len(_hour_cols)} hours"),
                    unsafe_allow_html=True)

    st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

    if _is_forecast_view:
        _heading_label = "Forecast volume by procedure &amp; hour"
    elif time_basis == "In-Lab":
        _heading_label = "In-lab volume by procedure &amp; hour"
    else:
        _heading_label = "Completed volume by procedure &amp; hour"
    st.markdown(f'<div class="section-heading">{_heading_label}</div>',
                unsafe_allow_html=True)

    if _is_forecast_view:
        _prefix = (
            f'Colour scale: &nbsp;'
            f'<strong style="color:{ORANGES_LOW};">■</strong> low &nbsp;→&nbsp; '
            f'<strong style="color:{ORANGES_HIGH};">■</strong> high '
            f'(hour columns only). &nbsp;'
            f'<strong>Total</strong> column = forecasted full-day sum per procedure.'
        )
    else:
        # Daily view: name what's being counted per hour. In-Lab vs
        # Completed shows different verbs ("in-lab volume" vs
        # "completions") since they're different measurements.
        _values_label = (
            "in-lab volume" if time_basis == "In-Lab" else "completions"
        )
        _prefix = (
            f'Values = {_values_label} per hour. '
            f'Colour scale: &nbsp;'
            f'<strong style="color:{VIRIDIS_LOW};">■</strong> low &nbsp;→&nbsp; '
            f'<strong style="color:{VIRIDIS_HIGH};">■</strong> high'
        )
    render_top_n_legend(_prefix)

    # Plotly heatmap (replaces the prior st.dataframe HTML table).
    if _is_forecast_view:
        _fig = build_analytics_heatmap(
            pivot,
            colorscale="Oranges",
            hovertemplate=(
                "<b>%{y} @ %{customdata[0]}</b><br>"
                "Forecast: %{customdata[1]:.1f}<extra></extra>"
            ),
        )
    else:
        # Build a procedure x hour avg-per-day pivot for the SAME month
        # (and time basis) so the hover tooltip can compare today's
        # count against the month average for that cell.
        _month_df = pd.DataFrame()
        try:
            if storage_is_configured():
                _m_start = date(selected_date.year, selected_date.month, 1)
                _m_end   = date(
                    selected_date.year, selected_date.month,
                    _cal.monthrange(selected_date.year, selected_date.month)[1],
                )
                _month_df = load_analytics_data(
                    start_date=_m_start,
                    end_date=_m_end,
                    resources=_current_resources,
                    time_basis=time_basis,
                    index_hash=_idx_hash,
                )
            else:
                _month_df = _local_df
        except Exception:
            _month_df = pd.DataFrame()
        _monthly_avg = load_monthly_avg_for_comparison(
            _month_df, selected_date.year, selected_date.month,
            cache_key=f"{map_type}|{time_basis}|{_idx_hash}",
        )

        _customdata = []
        for _proc in pivot.index:
            _row = []
            for _col in pivot.columns:
                _val = int(round(pivot.loc[_proc, _col]))
                if _col == "Total":
                    _row.append(f"Day total: {_val}")
                else:
                    _hour_int = LABEL_TO_HOUR.get(_col)
                    if (
                        _hour_int is not None
                        and not _monthly_avg.empty
                        and _proc in _monthly_avg.index
                        and _hour_int in _monthly_avg.columns
                    ):
                        _avg = float(_monthly_avg.loc[_proc, _hour_int])
                        _row.append(f"Today: {_val} | Month avg: {_avg:.1f}")
                    else:
                        _row.append(f"Today: {_val}")
            _customdata.append(_row)

        _fig = build_analytics_heatmap(
            pivot,
            colorscale="Viridis_r",
            hovertemplate=(
                "<b>%{y} @ %{customdata[0]}</b><br>"
                "%{customdata[1]}<extra></extra>"
            ),
            customdata=_customdata,
        )

    st.plotly_chart(
        _fig,
        use_container_width=True,
        key="analytics_daily_heatmap",
        config={
            "staticPlot": False,
            "scrollZoom": False,
            "displayModeBar": False,
        },
    )

    # Hourly Volume bar chart.
    st.markdown("---")

    st.markdown(
        f'<div class="section-heading">Hourly volume · {date_str}</div>',
        unsafe_allow_html=True,
    )

    _hourly = pivot[_hour_cols].sum().reset_index()
    _hourly.columns = ["Hour", "Total Volume"]
    _hourly_fig = go.Figure()
    _hourly_fig.add_trace(
        go.Bar(
            x=_hourly["Hour"],
            y=_hourly["Total Volume"],
            marker_color="#790A26",
            hovertemplate="<b>%{x}</b><br>Volume: %{y}<extra></extra>",
        )
    )
    _hourly_fig.update_layout(
        height=280,
        margin=dict(l=10, r=10, t=10, b=40),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        dragmode=False,
        xaxis=dict(
            tickfont=dict(size=10), title=None,
            fixedrange=True, categoryorder="array",
            categoryarray=list(_hour_cols),
        ),
        yaxis=dict(
            tickfont=dict(size=10), title="Total volume",
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
        ),
    )
    st.plotly_chart(
        _hourly_fig,
        use_container_width=True,
        key="analytics_daily_hourly",
        config={
            "staticPlot": False,
            "scrollZoom": False,
            "displayModeBar": False,
        },
    )
