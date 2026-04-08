"""
ui_components.py — CSS injection, header, metric cards, pivot styling, PNG export.

Contains all visual/presentation logic extracted from the monolithic app.py.
Changes from original:
  - Forecast heatmaps use Oranges colormap (issue #8)
  - Button text-shadow for better visibility (issue #9)
  - Login overlay CSS transition for instant hide (issue #10)
"""

import io
import calendar as _cal
from datetime import date

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import streamlit as st

from config import VMAX, HOUR_LABELS, NAVY, GOLD, STEEL


# ═════════════════════════════════════════════════════════════════════════════
# MATPLOTLIB FONT
# ═════════════════════════════════════════════════════════════════════════════

def setup_mpl_font() -> None:
    """Use Palatino if available, fall back to generic serif."""
    installed = {f.name for f in font_manager.fontManager.ttflist}
    for name in ("Palatino Linotype", "Palatino"):
        if name in installed:
            mpl.rcParams["font.family"] = name
            return
    mpl.rcParams["font.family"] = "serif"


# ═════════════════════════════════════════════════════════════════════════════
# CSS INJECTION
# ═════════════════════════════════════════════════════════════════════════════

def inject_css() -> None:
    """Inject all global CSS styles."""
    st.markdown(_GLOBAL_CSS, unsafe_allow_html=True)


_GLOBAL_CSS = """
<style>
/* ══════════════════════════════════════════════════════════
   BASE — background & font
   ══════════════════════════════════════════════════════════ */
html, body,
[data-testid="stAppViewContainer"],
[data-testid="stApp"],
.stApp,
section.main {
    font-family: 'Inter', system-ui, -apple-system, sans-serif !important;
    background-color: #f4f4f4 !important;
}
.block-container {
    padding-top: 1.8rem !important;
    padding-bottom: 2rem !important;
    max-width: 1480px !important;
}

/* ══════════════════════════════════════════════════════════
   SIDEBAR — dark background, light text
   ══════════════════════════════════════════════════════════ */
[data-testid="stSidebar"] {
    background-color: #1a1a1a !important;
    border-right: 1px solid #2e2e2e !important;
}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3 {
    color: #EDC153 !important;
    font-size: 0.72rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    margin-bottom: 0.25rem !important;
    margin-top: 0.1rem !important;
}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
[data-testid="stSidebar"] label {
    color: #e8e8e8 !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
}
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] small,
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
    color: #999999 !important;
    font-size: 0.73rem !important;
    font-weight: 400 !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
}
[data-testid="stSidebar"] hr {
    border-color: #2e2e2e !important;
    margin: 0.6rem 0 !important;
}

/* ══════════════════════════════════════════════════════════
   BUTTONS — USC Maroon with improved visibility (issue #9)
   ══════════════════════════════════════════════════════════ */
html body .stButton > button,
html body [data-testid="stBaseButton-secondary"],
html body [data-testid="stBaseButton-primary"],
html body [data-testid="stBaseButton-primaryFormSubmit"],
html body button[kind="primary"],
html body button[kind="secondary"],
html body button[kind="tertiary"] {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border: 1px solid #57121f !important;
    border-radius: 5px !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    padding: 0.4rem 1rem !important;
    box-shadow: none !important;
    transition: background-color 0.15s ease !important;
    text-shadow: 0 1px 1px rgba(0,0,0,0.2) !important;
}
html body .stButton > button:hover,
html body [data-testid="stBaseButton-secondary"]:hover,
html body [data-testid="stBaseButton-primary"]:hover,
html body [data-testid="stBaseButton-primaryFormSubmit"]:hover,
html body button[kind="primary"]:hover,
html body button[kind="secondary"]:hover {
    background-color: #57121f !important;
    color: #ffffff !important;
    border-color: #4a0f1b !important;
}
html body .stButton > button:disabled,
html body [data-testid="stBaseButton-secondary"]:disabled,
html body [data-testid="stBaseButton-primary"]:disabled {
    background-color: #b89099 !important;
    color: #ffffff !important;
    border-color: #b89099 !important;
    opacity: 0.65 !important;
    cursor: not-allowed !important;
}
/* Sidebar buttons — slightly lighter for contrast on dark bg */
[data-testid="stSidebar"] html body .stButton > button,
[data-testid="stSidebar"] .stButton > button {
    background-color: #8a2035 !important;
    border-color: #6F1828 !important;
    text-shadow: 0 1px 2px rgba(0,0,0,0.3) !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background-color: #6F1828 !important;
}

/* ══════════════════════════════════════════════════════════
   SELECTBOXES / DROPDOWNS
   ══════════════════════════════════════════════════════════ */
[data-testid="stSelectbox"] > div > div,
[data-testid="stSelectbox"] [role="combobox"],
[data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border: 1px solid #57121f !important;
    border-radius: 5px !important;
}
[data-testid="stSelectbox"] > div > div:focus-within,
[data-testid="stSelectbox"] > div > div[aria-expanded="true"] {
    background-color: #57121f !important;
    border-color: #4a0f1b !important;
}
[data-testid="stSelectbox"] > div > div > div,
[data-testid="stSelectbox"] [role="combobox"] > div {
    color: #ffffff !important;
}
[data-testid="stSelectbox"] svg {
    fill: #ffffff !important;
    color: #ffffff !important;
}
[role="listbox"] {
    background-color: #1a1a1a !important;
    border: 1px solid #3a3a3a !important;
    border-radius: 5px !important;
}
[role="option"] {
    color: #e8e8e8 !important;
    background-color: #1a1a1a !important;
}
[role="option"]:hover,
[role="option"][aria-selected="true"] {
    background-color: #6F1828 !important;
    color: #ffffff !important;
}

/* ══════════════════════════════════════════════════════════
   DATE INPUT
   ══════════════════════════════════════════════════════════ */
[data-testid="stDateInput"] input,
[data-testid="stSidebar"] [data-testid="stDateInput"] input {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border: 1px solid #57121f !important;
    border-radius: 5px !important;
}
[data-testid="stDateInput"] > div > div > div > svg {
    fill: #ffffff !important;
    color: #ffffff !important;
}

/* ══════════════════════════════════════════════════════════
   DATE PICKER POPUP
   ══════════════════════════════════════════════════════════ */
[data-baseweb="calendar"],
[data-baseweb="calendarContainer"] {
    background-color: #ffffff !important;
    color: #0f172a !important;
}
[data-baseweb="calendar"] div,
[data-baseweb="calendar"] span {
    background-color: #ffffff !important;
    color: #0f172a !important;
}
[data-baseweb="calendar"] [data-baseweb="select"] > div:first-child,
[data-baseweb="calendar"] [data-baseweb="select"] [role="combobox"],
[data-baseweb="calendar"] [data-baseweb="select"] [data-baseweb="input"] {
    background-color: #f1f5f9 !important;
    color: #0f172a !important;
    border: 1px solid #cbd5e1 !important;
    border-radius: 4px !important;
}
[data-baseweb="calendar"] [data-baseweb="select"] svg {
    fill: #0f172a !important;
    color: #0f172a !important;
}
[data-baseweb="calendar"] button {
    color: #0f172a !important;
    background-color: transparent !important;
}
[data-baseweb="calendar"] button:hover {
    background-color: #f0f0f0 !important;
}
[data-baseweb="calendar"] svg {
    fill: #0f172a !important;
    color: #0f172a !important;
}
[data-baseweb="calendar"] [data-testid="calendar-day-label"],
[data-baseweb="calendar"] abbr {
    color: #64748b !important;
    background-color: #ffffff !important;
}
[data-baseweb="calendar"] [role="gridcell"] button,
[data-baseweb="calendar"] [data-testid="calendar-day"] {
    color: #0f172a !important;
    background-color: transparent !important;
}
[data-baseweb="calendar"] [aria-selected="true"] button,
[data-baseweb="calendar"] [data-selected="true"] {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border-radius: 50% !important;
}
[data-baseweb="calendar"] [aria-selected="true"] div,
[data-baseweb="calendar"] [aria-selected="true"] span {
    background-color: transparent !important;
    color: #ffffff !important;
}
[data-baseweb="calendar"] [data-today="true"] button {
    border: 2px solid #6F1828 !important;
    border-radius: 50% !important;
}
[data-baseweb="calendar"] [aria-disabled="true"] button {
    color: #cccccc !important;
}

/* ══════════════════════════════════════════════════════════
   RADIO BUTTONS
   ══════════════════════════════════════════════════════════ */
[data-testid="stRadio"] label {
    background-color: transparent !important;
    box-shadow: none !important;
    border: none !important;
}
[data-testid="stRadio"] label p {
    color: #e8e8e8 !important;
    font-weight: 500 !important;
    font-size: 0.88rem !important;
    background-color: transparent !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
}
[data-testid="stRadio"] label:hover {
    background-color: transparent !important;
}

/* ══════════════════════════════════════════════════════════
   TEXT INPUTS
   ══════════════════════════════════════════════════════════ */
[data-testid="stTextInput"] input,
[data-testid="stSidebar"] [data-testid="stTextInput"] input {
    background-color: #ffffff !important;
    color: #111111 !important;
    border: 1px solid #cccccc !important;
    border-radius: 5px !important;
    font-size: 0.9rem !important;
}
[data-testid="stTextInput"] input:focus,
[data-testid="stSidebar"] [data-testid="stTextInput"] input:focus {
    border-color: #6F1828 !important;
    outline: none !important;
    box-shadow: 0 0 0 2px rgba(111,24,40,0.15) !important;
}

/* ══════════════════════════════════════════════════════════
   METRIC CARDS
   ══════════════════════════════════════════════════════════ */
[data-testid="metric-container"] {
    background: #ffffff !important;
    border: 1px solid #e2e8f0 !important;
    border-top: 3px solid #6F1828 !important;
    border-radius: 8px !important;
    padding: 1rem 1.25rem !important;
    box-shadow: 0 1px 3px rgba(0,0,0,0.07) !important;
}
[data-testid="metric-container"] label {
    font-size: 0.68rem !important;
    font-weight: 700 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    color: #64748b !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 1.55rem !important;
    font-weight: 700 !important;
    color: #0f172a !important;
}

/* ══════════════════════════════════════════════════════════
   EXPANDERS
   ══════════════════════════════════════════════════════════ */
[data-testid="stExpander"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
    background: #ffffff !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
}
[data-testid="stExpander"] summary {
    font-weight: 600 !important;
    color: #0f172a !important;
    font-size: 0.88rem !important;
}

/* Sidebar expanders have a WHITE background — undo the sidebar's global
   light-text rules for anything rendered inside them, otherwise labels
   and captions become invisible (light gray on white). */
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] p,
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] li,
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] span,
[data-testid="stSidebar"] [data-testid="stExpander"] label,
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stWidgetLabel"],
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stWidgetLabel"] p {
    color: #0f172a !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stCaptionContainer"] p,
[data-testid="stSidebar"] [data-testid="stExpander"] small {
    color: #475569 !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] strong {
    color: #0f172a !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] summary,
[data-testid="stSidebar"] [data-testid="stExpander"] summary p,
[data-testid="stSidebar"] [data-testid="stExpander"] summary span {
    color: #0f172a !important;
}
/* File uploader "drag and drop" area and hint text inside sidebar expander */
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stFileUploaderDropzone"] {
    background: #f8fafc !important;
    border: 1px dashed #94a3b8 !important;
}
[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stFileUploaderDropzone"] * {
    color: #0f172a !important;
}

/* ══════════════════════════════════════════════════════════
   DIVIDERS
   ══════════════════════════════════════════════════════════ */
hr {
    border-color: #e2e8f0 !important;
    margin: 1.1rem 0 !important;
}

/* ══════════════════════════════════════════════════════════
   CUSTOM COMPONENT CLASSES
   ══════════════════════════════════════════════════════════ */
.keck-header {
    background: linear-gradient(135deg, #6F1828 0%, #521322 100%);
    padding: 1.2rem 2rem;
    border-radius: 10px;
    margin-bottom: 1.4rem;
    display: flex;
    align-items: center;
    justify-content: space-between;
    box-shadow: 0 2px 8px rgba(111,24,40,0.25);
}
.keck-header h1 {
    color: #ffffff;
    font-size: 1.5rem;
    font-weight: 700;
    margin: 0;
    letter-spacing: 0.2px;
}
.keck-header .subtitle {
    color: #EDC153;
    font-size: 0.87rem;
    margin: 0.25rem 0 0;
    opacity: 0.95;
    font-weight: 500;
}
.keck-badge {
    background: #EDC153;
    color: #3a1a00;
    font-size: 0.68rem;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 12px;
    letter-spacing: 1px;
    text-transform: uppercase;
}
.keck-date-label {
    color: #d0d8e4;
    font-size: 0.78rem;
    margin-top: 6px;
    display: block;
}
.metric-card {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-top: 4px solid #6F1828;
    border-radius: 8px;
    padding: 1rem 1.1rem;
    margin-bottom: 0.5rem;
    height: 100%;
    box-shadow: 0 1px 4px rgba(0,0,0,0.06);
}
.metric-card.accent { border-top-color: #EDC153; }
.metric-card .label {
    color: #64748b;
    font-size: 0.7rem;
    font-weight: 600;
    text-transform: uppercase;
    letter-spacing: 0.6px;
    margin-bottom: 0.3rem;
}
.metric-card .value {
    color: #6F1828;
    font-size: 1.5rem;
    font-weight: 700;
    line-height: 1.1;
    word-break: break-word;
}
.metric-card .sub {
    color: #64748b;
    font-size: 0.72rem;
    margin-top: 0.25rem;
}
.section-heading {
    color: #6F1828;
    font-size: 1.05rem;
    font-weight: 700;
    border-bottom: 2px solid #EDC153;
    padding-bottom: 0.3rem;
    margin: 1rem 0 0.8rem;
}
.heatmap-legend {
    background: #ffffff;
    border: 1px solid #e2e8f0;
    border-left: 3px solid #EDC153;
    border-radius: 6px;
    padding: 0.5rem 0.85rem;
    font-size: 0.78rem;
    color: #64748b;
    margin-bottom: 0.7rem;
}
.status-chip {
    display: inline-block;
    background: #DCFCE7;
    color: #166534;
    font-size: 0.7rem;
    font-weight: 600;
    padding: 3px 9px;
    border-radius: 10px;
    margin-bottom: 0.4rem;
}
.status-chip.warn  { background: #FEF9C3; color: #854D0E; }
.status-chip.error { background: #FEE2E2; color: #991B1B; }
.metrics-divider {
    border: none;
    border-top: 1px solid #e2e8f0;
    margin: 0.9rem 0 1.1rem;
}
.refresh-btn button {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    font-weight: 600 !important;
    border: none !important;
    border-radius: 6px !important;
    width: 100% !important;
}
div[data-testid="stDataFrame"] {
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
}

/* ══════════════════════════════════════════════════════════
   LOGIN OVERLAY — instant hide after auth (issue #10)
   ══════════════════════════════════════════════════════════ */
#login-overlay {
    transition: opacity 0.2s ease, transform 0.2s ease;
}
#login-overlay.hiding {
    opacity: 0;
    transform: translateY(-10px);
    pointer-events: none;
}
</style>
"""


# ═════════════════════════════════════════════════════════════════════════════
# UI HELPER FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════

def metric_card(label: str, value: str, sub: str = "", accent: bool = False) -> str:
    """Return an HTML string for a styled metric card."""
    cls = "metric-card accent" if accent else "metric-card"
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    return (
        f'<div class="{cls}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>'
        f'{sub_html}'
        f'</div>'
    )


def render_header(map_type: str, date_str: str) -> None:
    """Render the branded Keck Medicine header banner."""
    st.markdown(f"""
    <div class="keck-header">
      <div>
        <h1>Productivity Dashboard</h1>
        <p class="subtitle">{map_type}</p>
      </div>
      <div style="text-align:right;">
        <span class="keck-badge">Analytics</span>
        <span class="keck-date-label">{date_str}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)


def status_chip(text: str, level: str = "ok") -> None:
    """Render a small coloured status chip (level: 'ok', 'warn', or 'error')."""
    cls = {"ok": "status-chip", "warn": "status-chip warn", "error": "status-chip error"}
    st.markdown(
        f'<div class="{cls.get(level, "status-chip")}">{text}</div>',
        unsafe_allow_html=True,
    )


# ═════════════════════════════════════════════════════════════════════════════
# PIVOT STYLING
# ═════════════════════════════════════════════════════════════════════════════

def style_pivot(pivot: pd.DataFrame, vmax: int, cmap: str = "viridis_r"):
    """Apply colormap background-gradient styling to the pivot DataFrame."""
    hour_cols = [c for c in pivot.columns if c != "Total"]
    return (
        pivot.style
        .background_gradient(cmap=cmap, vmin=0, vmax=vmax, subset=hour_cols)
        .format("{:.0f}")
        .set_properties(**{"text-align": "center"})
        .set_properties(subset=["Total"], **{
            "font-weight": "bold",
            "font-size":   "13px",
            "border-left": "2px solid #888",
        })
        .set_table_styles([
            {"selector": "th",            "props": [("text-align", "center"), ("font-size", "12px")]},
            {"selector": "th.row_heading","props": [("text-align", "left"),   ("min-width", "220px")]},
        ])
    )


def style_forecast_pivot(pivot: pd.DataFrame, vmax: int):
    """Style forecast pivot with Oranges colormap (issue #8)."""
    return style_pivot(pivot, vmax, cmap="Oranges")


def style_monthly_pivot(pivot: pd.DataFrame, vmax: int):
    """Apply viridis_r gradient styling to a monthly average pivot."""
    hour_cols = [c for c in pivot.columns if c != "Total"]
    return (
        pivot.style
        .background_gradient(cmap="viridis_r", vmin=0, vmax=vmax, subset=hour_cols)
        .format(lambda v: str(int(round(v))))
        .set_properties(**{"text-align": "center"})
        .set_properties(subset=["Total"], **{
            "font-weight": "bold",
            "font-size":   "13px",
            "border-left": "2px solid #888",
        })
        .set_table_styles([
            {"selector": "th",            "props": [("text-align", "center"), ("font-size", "12px")]},
            {"selector": "th.row_heading","props": [("text-align", "left"),   ("min-width", "220px")]},
        ])
    )


# ═════════════════════════════════════════════════════════════════════════════
# PNG EXPORT
# ═════════════════════════════════════════════════════════════════════════════

def build_png(
    df_dh: pd.DataFrame,
    map_type: str,
    selected_date: date,
    hours: list[int],
    is_forecast: bool = False,
) -> bytes:
    """Render the heatmap as a high-DPI PNG and return raw PNG bytes."""
    vmax    = VMAX[map_type]
    n_hours = len(hours)
    cmap    = "Oranges" if is_forecast else "viridis_r"

    raw = (
        df_dh.pivot_table(
            index="Order Procedure", columns="hour",
            values="Complete Volume", aggfunc="sum", fill_value=0.0,
        ).reindex(columns=hours, fill_value=0.0)
    )
    raw["__total__"] = raw.sum(axis=1)
    raw = raw.sort_values("__total__", ascending=False)
    row_totals = raw["__total__"].values
    raw = raw.drop(columns=["__total__"])

    mat     = raw.to_numpy()
    ylabels = raw.index.tolist()
    xlabels = [HOUR_LABELS[h] for h in hours]

    fig_w = max(10, 0.6 * n_hours)
    fig_h = max(5,  0.35 * len(ylabels))
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)

    im = ax.imshow(mat, aspect="auto", cmap=cmap, vmin=0, vmax=vmax)
    label_prefix = "Forecast — " if is_forecast else ""
    ax.set_title(
        f"{label_prefix}{map_type} – Top 30 Procedures  |  "
        f"{pd.Timestamp(selected_date).strftime('%B %d, %Y')}",
        fontsize=11, pad=10,
    )
    ax.set_xticks(np.arange(n_hours))
    ax.set_xticklabels(xlabels, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(np.arange(len(ylabels)))
    ax.set_yticklabels(ylabels, fontsize=8)

    cbar = fig.colorbar(im, ax=ax, fraction=0.02, pad=0.005)
    cbar.set_label("Completed volume in hour", fontsize=8)

    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            ax.text(j, i, f"{mat[i, j]:.0f}", ha="center", va="center",
                    fontsize=6.5, color="black")

    total_x = n_hours - 0.25
    ax.text(total_x, -0.75, "Total", ha="center", va="center",
            fontsize=8, fontweight="bold", transform=ax.transData)
    for i, t in enumerate(row_totals):
        ax.text(total_x, i, f"{t:.0f}", ha="center", va="center",
                fontsize=8, fontweight="bold", transform=ax.transData)
    ax.set_xlim(-0.5, n_hours + 0.25)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=250, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf.getvalue()


def build_monthly_png(
    monthly_pivot: pd.DataFrame,
    map_type: str,
    year: int,
    month: int,
    n_days: int,
) -> bytes:
    """Render the monthly average heatmap as a high-DPI PNG."""
    vmax      = VMAX[map_type]
    hour_cols = [c for c in monthly_pivot.columns if c != "Total"]
    n_hours   = len(hour_cols)

    mat        = monthly_pivot[hour_cols].to_numpy()
    row_totals = monthly_pivot["Total"].values
    ylabels    = monthly_pivot.index.tolist()
    month_label = f"{_cal.month_name[month]} {year}"

    fig_w = max(10, 0.6 * n_hours)
    fig_h = max(5,  0.35 * len(ylabels))
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)

    im = ax.imshow(mat, aspect="auto", cmap="viridis_r", vmin=0, vmax=vmax)
    ax.set_title(
        f"{map_type} – Monthly Average (Top 30)  |  "
        f"{month_label}  |  N = {n_days} days",
        fontsize=11, pad=10,
    )
    ax.set_xticks(np.arange(n_hours))
    ax.set_xticklabels(hour_cols, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(np.arange(len(ylabels)))
    ax.set_yticklabels(ylabels, fontsize=8)

    cbar = fig.colorbar(im, ax=ax, fraction=0.02, pad=0.005)
    cbar.set_label("Avg completed volume per day in hour", fontsize=8)

    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            ax.text(j, i, f"{mat[i, j]:.1f}", ha="center", va="center",
                    fontsize=6.5, color="black")

    total_x = n_hours - 0.25
    ax.text(total_x, -0.75, "Total", ha="center", va="center",
            fontsize=8, fontweight="bold", transform=ax.transData)
    for i, t in enumerate(row_totals):
        ax.text(total_x, i, f"{t:.1f}", ha="center", va="center",
                fontsize=8, fontweight="bold", transform=ax.transData)
    ax.set_xlim(-0.5, n_hours + 0.25)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=250, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf.getvalue()


def build_weekday_png(
    weekday_pivot: pd.DataFrame,
    map_type: str,
    year: int,
    month: int,
) -> bytes:
    """Render the weekday x hour average heatmap as a high-DPI PNG."""
    hour_cols  = [c for c in weekday_pivot.columns if c != "Total"]
    n_hours    = len(hour_cols)
    mat        = weekday_pivot[hour_cols].to_numpy()
    row_totals = weekday_pivot["Total"].values
    ylabels    = weekday_pivot.index.tolist()
    vmax       = max(1, int(mat.max()))
    month_label = f"{_cal.month_name[month]} {year}"

    fig_w = max(10, 0.6 * n_hours)
    fig_h = max(3,  0.55 * len(ylabels))
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)

    im = ax.imshow(mat, aspect="auto", cmap="viridis_r", vmin=0, vmax=vmax)
    ax.set_title(
        f"{map_type} — Weekday Pattern  |  {month_label}",
        fontsize=11, pad=10,
    )
    ax.set_xticks(np.arange(n_hours))
    ax.set_xticklabels(hour_cols, rotation=45, ha="right", fontsize=8)
    ax.set_yticks(np.arange(len(ylabels)))
    ax.set_yticklabels(ylabels, fontsize=9)

    cbar = fig.colorbar(im, ax=ax, fraction=0.02, pad=0.005)
    cbar.set_label("Avg completed volume per weekday occurrence", fontsize=8)

    for i in range(mat.shape[0]):
        for j in range(mat.shape[1]):
            ax.text(j, i, str(int(round(mat[i, j]))), ha="center", va="center",
                    fontsize=6.5, color="black")

    total_x = n_hours - 0.25
    ax.text(total_x, -0.75, "Total", ha="center", va="center",
            fontsize=8, fontweight="bold", transform=ax.transData)
    for i, t in enumerate(row_totals):
        ax.text(total_x, i, str(int(round(t))), ha="center", va="center",
                fontsize=8, fontweight="bold", transform=ax.transData)
    ax.set_xlim(-0.5, n_hours + 0.25)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=250, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf.getvalue()
