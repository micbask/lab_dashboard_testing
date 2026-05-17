"""
analytics/views/_shared.py — Constants and helpers shared by TAT, Daily,
and Monthly view modules.

Houses anything that more than one view needs:
  • Plotly colour-scale endpoint colours for the legend swatches.
  • The Top-N selector options and validator tuple.
  • Per-priority TAT colours used by the legend, summary table,
    per-procedure table headers, and bar chart.
  • build_analytics_heatmap — the canonical Plotly heatmap builder
    used by every analytics heatmap (Daily, Monthly, Forecast).
  • render_top_n_legend — the inline legend + Top-N button row.
  • apply_local_file_scope — bench/proc/inlab filter for the
    no-storage local-file fallback path.

Private to the views package; the page controller (analytics/dashboard.py)
imports from here too.
"""

import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from analytics.filters import EXCLUDED_PROCEDURES


# ─── Plotly colorscale legend swatches ───────────────────────────────
# Viridis_r / Oranges endpoint hex codes — picked off Plotly's built-in
# colorscales so the small legend swatch matches the actual gradient.
# Viridis is REVERSED on analytics heatmaps (Viridis_r) so dark = high.
VIRIDIS_LOW  = "#fde725"   # bright yellow — Viridis_r low end
VIRIDIS_HIGH = "#440154"   # deep purple   — Viridis_r high end
ORANGES_LOW  = "#fff5eb"
ORANGES_HIGH = "#7f2704"

# Solid neutral fill for the Total column on every analytics heatmap.
# Keeps Total cells out of the colorscale so a wide-range full-day sum
# doesn't compress the per-hour gradient.
TOTAL_NEUTRAL = "#ececec"

# ─── Per-priority TAT colours ────────────────────────────────────────
# Used across the TAT legend, summary table, per-procedure table
# headers, and bar chart so RT/ST/TS/All stay visually grouped end-to-
# end. TS gets teal — distinct from RT's blue and ST's warm orange.
# All stays neutral grey.
TAT_ROUTINE_COLOR  = "#0066cc"   # RT (Routine)
TAT_STAT_COLOR     = "#cc6600"   # ST (Stat)
TAT_TS_COLOR       = "#0a9396"   # TS (Time Study)
TAT_COMBINED_COLOR = "#444444"   # All

# ─── Top-N selector options ──────────────────────────────────────────
# Each tuple is (label, value). `value` is the int top-N passed
# downstream to build_pivot / build_monthly_pivot / build_forecast_pivot;
# None means "no top-N filter, return every procedure" (the "All" button).
TOP_N_OPTIONS = [("10", 10), ("20", 20), ("30", 30), ("All", None)]
VALID_TOP_N = tuple(v for _, v in TOP_N_OPTIONS)


def build_analytics_heatmap(
    pivot: pd.DataFrame,
    *,
    colorscale: str,
    hovertemplate: str,
    customdata=None,
) -> go.Figure:
    """Render the analytics dashboard heatmap as a Plotly figure.

    Mirrors the pre_analytics conventions: integer text labels in cells,
    `xgap/ygap=1`, no colorbar (`showscale=False`), `dragmode=False`,
    locked axis ranges, transparent background, and a white hover tooltip
    matching the rest of the app.

    Two `go.Heatmap` traces share a single numeric x-axis so the chart
    renders as one continuous heatmap:

      • Hour columns (24 cells) — coloured with `colorscale`. `zmax`
        is the 95th percentile of non-zero hour cells, so wide-range
        full-day totals never get to compress the per-hour gradient.
        Hour cells above zmax clip to the high gradient colour
        (Plotly's default zmax behaviour).
      • Total column (1 cell)   — rendered with a flat neutral grey
        via a 2-stop single-colour colorscale, completely independent
        of the hour gradient.

    Numeric x-coords (0..23 for hours, 24 for Total) are used because
    using categorical x labels for two side-by-side traces caused the
    chart to render at ~half width. Tick labels are restored via
    `tickvals`/`ticktext`.

    `customdata`, when supplied, must match the pivot's full shape
    (n_rows × n_cols). Each cell value is paired with its column label
    so hovertemplates can reference `%{customdata[0]}` (column label)
    and `%{customdata[1]}` (cell value or pre-formatted string). When
    `customdata` is None, the cell values default to the original z.
    """
    z_full = pivot.values.astype(float)
    y = pivot.index.tolist()
    cols = pivot.columns.tolist()

    if "Total" in cols:
        total_idx    = cols.index("Total")
        hour_indices = [i for i in range(len(cols)) if i != total_idx]
        hour_cols    = [cols[i] for i in hour_indices]
        z_hours      = z_full[:, hour_indices]
        z_total      = z_full[:, [total_idx]]
    else:
        total_idx    = None
        hour_indices = list(range(len(cols)))
        hour_cols    = cols
        z_hours      = z_full
        z_total      = None

    cell_values = z_full if customdata is None else customdata

    cd_hours = [
        [[hour_cols[j], cell_values[i][hour_indices[j]]]
         for j in range(len(hour_indices))]
        for i in range(len(y))
    ]
    cd_total = None
    if total_idx is not None:
        cd_total = [
            [["Total", cell_values[i][total_idx]]]
            for i in range(len(y))
        ]

    _nz = z_hours[z_hours > 0]
    zmax_hours = float(np.percentile(_nz, 95)) if _nz.size else 1.0
    zmax_hours = max(zmax_hours, 1.0)

    text_hours = [
        [str(int(round(v))) if v > 0 else "" for v in row]
        for row in z_hours
    ]

    x_hours_coords = list(range(len(hour_cols)))
    x_total_coord  = len(hour_cols)

    fig = go.Figure()
    fig.add_trace(
        go.Heatmap(
            z=z_hours,
            x=x_hours_coords,
            y=y,
            text=text_hours,
            texttemplate="%{text}",
            hoverinfo="text",
            colorscale=colorscale,
            zmin=0,
            zmax=zmax_hours,
            xgap=1,
            ygap=1,
            showscale=False,
            hovertemplate=hovertemplate,
            customdata=cd_hours,
        )
    )

    if z_total is not None:
        text_total = [
            [str(int(round(v))) if v > 0 else "" for v in row]
            for row in z_total
        ]
        fig.add_trace(
            go.Heatmap(
                z=z_total,
                x=[x_total_coord],
                y=y,
                text=text_total,
                texttemplate="%{text}",
                hoverinfo="text",
                colorscale=[[0.0, TOTAL_NEUTRAL], [1.0, TOTAL_NEUTRAL]],
                zmin=0,
                zmax=1,
                xgap=1,
                ygap=1,
                showscale=False,
                hovertemplate=hovertemplate,
                customdata=cd_total,
                textfont=dict(color="#1a1a1a", size=11),
            )
        )

    fig.update_traces(hoverongaps=False)

    tick_vals = list(x_hours_coords)
    tick_text = list(hour_cols)
    if z_total is not None:
        tick_vals.append(x_total_coord)
        tick_text.append("Total")

    plot_h = max(320, len(y) * 28 + 100)
    fig.update_layout(
        height=plot_h,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        dragmode=False,
        xaxis=dict(
            tickmode="array",
            tickvals=tick_vals,
            ticktext=tick_text,
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
    return fig


def render_top_n_legend(prefix_html: str) -> None:
    """Render legend prose + Top-N selector inline using st.columns.

    `prefix_html` is the legend prose (e.g. "Colour scale: ... full-day
    sum per procedure."). Appends a "Showing top" label column and four
    st.button columns (10 / 20 / 30 / All). Selected button uses
    type="primary" so CSS can target it for cardinal+gold styling.

    Native Streamlit widgets are required: clicks trigger script reruns
    (preserve session_state including auth) rather than the full-page
    navigation that a prior <a href="?top_n=N"> design caused.
    """
    current_n = st.session_state.get("analytics_top_n", 10)
    _cols = st.columns(
        [6, 0.7, 0.3, 0.3, 0.3, 0.4],
        vertical_alignment="center",
    )
    with _cols[0]:
        st.markdown(
            f'<div class="heatmap-legend-inline">{prefix_html}</div>',
            unsafe_allow_html=True,
        )
    with _cols[1]:
        st.markdown(
            '<div class="top-n-label">Showing top</div>',
            unsafe_allow_html=True,
        )
    for _col, (_label, _value) in zip(_cols[2:], TOP_N_OPTIONS):
        with _col:
            _is_sel = (_value == current_n)
            if st.button(
                _label,
                key=f"top_n_btn_{_label}",
                type="primary" if _is_sel else "secondary",
                use_container_width=True,
            ):
                st.session_state["analytics_top_n"] = _value
                st.rerun()


def apply_local_file_scope(
    local_df: pd.DataFrame,
    resources: list,
    time_basis: str,
) -> pd.DataFrame:
    """Apply bench-level scope + procedure exclusions + In-Lab remap
    to a local-file-upload DataFrame.

    Mirrors what `load_analytics_data` does on the storage path: scopes
    to the testing bench's resources, removes excluded procedures, and
    (for time_basis="In-Lab") re-maps complete_date/hour to inlab_date/
    inlab_hour while dropping rows without an In-Lab timestamp.

    Only used when storage is not configured; the storage path runs this
    inside the cached loader instead.
    """
    out = local_df[
        local_df["Performing Service Resource"].isin(resources) &
        ~local_df["Order Procedure"].isin(EXCLUDED_PROCEDURES)
    ].copy()
    if time_basis == "In-Lab":
        _has_inlab = (
            "inlab_date" in out.columns
            and out["inlab_date"].notna().any()
        )
        if not _has_inlab:
            return out.iloc[0:0]
        out = out[out["inlab_date"].notna()].copy()
        out["complete_date"] = out["inlab_date"]
        out["hour"] = out["inlab_hour"].astype(int)
    return out
