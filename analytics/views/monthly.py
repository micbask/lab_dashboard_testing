"""
analytics/views/monthly.py — Monthly heatmap + day-of-week breakdown.

Renders:
  • 4 KPI cards (Total volume, Top procedure, Peak hour, Avg/day).
  • Procedure × hour-of-day monthly-average heatmap.
  • Weekday KPI strip (Busiest day, Peak hour, Lightest day).
  • Day-of-week × hour-of-day heatmap.

Extracted from analytics/dashboard.py in Batch 5 Phase 2.
"""

import calendar as _cal
from datetime import date

import streamlit as st

from storage import storage_is_configured
from ui_components import metric_card, render_header
from analytics.data import (
    load_analytics_data, build_monthly_pivot, build_weekday_pivot,
)
from analytics.views._shared import (
    VIRIDIS_LOW, VIRIDIS_HIGH,
    build_analytics_heatmap, render_top_n_legend, apply_local_file_scope,
)


def render_monthly_view(params: dict, ss) -> None:
    """Render the Monthly heatmap, KPI strip, and day-of-week breakdown."""
    map_type           = params["map_type"]
    time_basis         = params["time_basis"]
    _local_df          = params["local_df"]
    _current_resources = params["current_resources"]
    _idx_hash          = params["idx_hash"]
    selected_year      = params["selected_year"]
    selected_month     = params["selected_month"]

    month_name_str = f"{_cal.month_name[selected_month]} {selected_year}"
    render_header(map_type, month_name_str)

    _month_start = date(selected_year, selected_month, 1)
    _month_end   = date(selected_year, selected_month,
                        _cal.monthrange(selected_year, selected_month)[1])

    if storage_is_configured():
        filtered_df = load_analytics_data(
            start_date=_month_start,
            end_date=_month_end,
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

    monthly_pivot, n_days, month_raw_df = build_monthly_pivot(
        filtered_df, selected_year, selected_month,
        top_n=st.session_state.get("analytics_top_n", 10),
    )

    if monthly_pivot is None:
        st.warning(f"No data found for **{map_type}** in **{month_name_str}**.")
        return

    _m_hour_cols   = [c for c in monthly_pivot.columns if c != "Total"]
    # Total volume = sum across the FULL filtered DataFrame (not the
    # top-N pivot). monthly_pivot is post-top-N filtered and its
    # "Total" column is the per-procedure per-day AVERAGE, so
    # `sum() * n_days` only covers the visible procedures and
    # under-reports actual completions whenever the user picks
    # Top 10/20/30 instead of "All".
    _m_total_vol   = int(filtered_df["Complete Volume"].fillna(0).sum())
    _m_top_proc    = monthly_pivot["Total"].idxmax()
    _m_peak_col    = monthly_pivot[_m_hour_cols].sum().idxmax()
    _m_peak_disp   = _m_peak_col.replace("AM", " AM").replace("PM", " PM")
    _m_avg_per_day = round(_m_total_vol / max(n_days, 1))

    _mm1, _mm2, _mm3, _mm4 = st.columns(4)
    with _mm1:
        st.markdown(metric_card("Total volume", f"{_m_total_vol:,}", accent=True),
                    unsafe_allow_html=True)
    with _mm2:
        st.markdown(metric_card("Top procedure", _m_top_proc,
                    sub=f"highest volume in {month_name_str}"),
                    unsafe_allow_html=True)
    with _mm3:
        st.markdown(metric_card("Peak hour", _m_peak_disp,
                    sub="highest avg volume"), unsafe_allow_html=True)
    with _mm4:
        st.markdown(metric_card("Avg / day", f"{_m_avg_per_day:,}",
                    sub=f"over {n_days} days"), unsafe_allow_html=True)

    st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

    st.markdown(
        f'<div class="section-heading">'
        f'{map_type} - monthly average · {month_name_str} · N = {n_days} days'
        f'</div>',
        unsafe_allow_html=True,
    )
    _m_values_label = (
        "in-lab" if time_basis == "In-Lab" else "completed"
    )
    _m_prefix = (
        f'Values = avg {_m_values_label} volume per day in hour. '
        f'Colour scale: &nbsp;'
        f'<strong style="color:{VIRIDIS_LOW};">■</strong> low &nbsp;→&nbsp; '
        f'<strong style="color:{VIRIDIS_HIGH};">■</strong> high'
    )
    render_top_n_legend(_m_prefix)

    _m_fig = build_analytics_heatmap(
        monthly_pivot,
        colorscale="Viridis_r",
        text_decimals=0,
        hovertemplate=(
            "<b>%{y} @ %{customdata[0]}</b><br>"
            "Avg per day: %{customdata[1]:.1f}<extra></extra>"
        ),
    )
    st.plotly_chart(
        _m_fig,
        use_container_width=True,
        key="analytics_monthly_heatmap",
        config={
            "staticPlot": False,
            "scrollZoom": False,
            "displayModeBar": False,
        },
    )

    # Weekday-pattern Plotly heatmap.
    st.markdown("---")

    weekday_pivot, _wd_counts = build_weekday_pivot(
        month_raw_df, selected_year, selected_month,
        cache_key=f"{map_type}|{time_basis}|{_idx_hash}",
    )

    if weekday_pivot is None:
        st.info("No data available for weekday breakdown.")
        return

    _wd_hour_cols    = [c for c in weekday_pivot.columns if c != "Total"]
    _wd_busiest_day  = weekday_pivot["Total"].idxmax()
    _wd_lightest_day = weekday_pivot["Total"].idxmin()
    _wd_peak_hour    = weekday_pivot[_wd_hour_cols].sum().idxmax()
    _wd_peak_disp    = _wd_peak_hour.replace("AM", " AM").replace("PM", " PM")

    _wc1, _wc2, _wc3 = st.columns(3)
    with _wc1:
        st.markdown(
            metric_card(
                "Busiest day",
                _wd_busiest_day.split("  ")[0],
                sub=f"avg {int(round(weekday_pivot.loc[_wd_busiest_day, 'Total']))} vol / day",
            ),
            unsafe_allow_html=True,
        )
    with _wc2:
        st.markdown(
            metric_card("Peak hour", _wd_peak_disp,
                        sub="highest avg volume across weekdays"),
            unsafe_allow_html=True,
        )
    with _wc3:
        st.markdown(
            metric_card(
                "Lightest day",
                _wd_lightest_day.split("  ")[0],
                sub=f"avg {int(round(weekday_pivot.loc[_wd_lightest_day, 'Total']))} vol / day",
            ),
            unsafe_allow_html=True,
        )

    st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

    st.markdown(
        f'<div class="section-heading">'
        f'{map_type} - day-of-week pattern · {month_name_str}'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="heatmap-legend">'
        f'Values = avg {_m_values_label} volume per day in hour. '
        f'Colour scale: &nbsp;'
        f'<strong style="color:{VIRIDIS_LOW};">■</strong> low &nbsp;→&nbsp; '
        f'<strong style="color:{VIRIDIS_HIGH};">■</strong> high'
        f'</div>',
        unsafe_allow_html=True,
    )

    _wd_fig = build_analytics_heatmap(
        weekday_pivot,
        colorscale="Viridis_r",
        text_decimals=0,
        hovertemplate=(
            "<b>%{y} @ %{customdata[0]}</b><br>"
            "Avg: %{customdata[1]:.1f}<extra></extra>"
        ),
    )
    _wd_n = len(weekday_pivot)
    _wd_fig.update_layout(
        height=_wd_n * 28 + 40,
        margin=dict(l=10, r=10, t=10, b=30),
    )
    st.plotly_chart(
        _wd_fig,
        use_container_width=True,
        key="analytics_weekday_heatmap",
        config={
            "staticPlot": False,
            "scrollZoom": False,
            "displayModeBar": False,
        },
    )
