import io
from copy import deepcopy

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import streamlit as st

# ─────────────────────────────────────────────────────────────────────────────
# Page config
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Lab Productivity Heatmaps",
    page_icon=":microscope:",
    layout="wide",
)

st.markdown("""
<style>
    .block-container { padding-top: 1.5rem; }
    div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────────────────
# Font
# ─────────────────────────────────────────────────────────────────────────────
def set_preferred_font():
    installed = {f.name for f in font_manager.fontManager.ttflist}
    for name in ("Palatino Linotype", "Palatino"):
        if name in installed:
            mpl.rcParams["font.family"] = name
            return
    mpl.rcParams["font.family"] = "serif"

set_preferred_font()

# ─────────────────────────────────────────────────────────────────────────────
# Configuration
# ─────────────────────────────────────────────────────────────────────────────
DEFAULT_RESOURCES = {
    "Keck Core": [
        "Keck Abbott DI", "Keck Coagulation", "Keck Cobas",
        "Keck HEME Orders", "Keck IRIS", "Keck ISED", "Keck SmartLyte A",
        "Keck TEG 5000", "Keck Urinalysis", "USC Manual Coagulation Bench",
        "USC Manual Hematology Bench", "USC Manual Urinalysis Bench",
        "USC Serology Routine Bench",
    ],
    "Norris Core": [
        "NCH Coagulation", "NCH COBAS", "NCH HEME Orders", "NCH IRIS",
        "NCI Manual Chemistry Bench", "NCI Manual Hematology Bench",
        "NCI Stem Cell Bench", "NCH Cobas PRO A", "NCH Cobas PRO B",
        "NCH GEM 4000 H", "NCH GEM 4000 I",
    ],
    "Norris Specialty": [
        "NCH DS2 A", "NCH HydraSys", "NCH PFA 100", "NCH Tosoh G8",
        "NCI Manual Flow Bench", "NCI Manual Verify Now Bench",
    ],
}

VMAX = {"Keck Core": 50, "Norris Core": 30, "Norris Specialty": 20}

EXCLUDE_PROCS = {
    "Glomerular Filtration Rate Estimated",
    ".Diff Auto -",
    "Manual Diff-",
}

ALL_RESOURCES = sorted({r for rs in DEFAULT_RESOURCES.values() for r in rs})


def hour_label(h: int) -> str:
    """0 → '12AM', 1 → '1AM', 13 → '1PM', etc."""
    hr12 = 12 if h % 12 == 0 else h % 12
    suffix = "AM" if h < 12 else "PM"
    return f"{hr12}{suffix}"


HOUR_LABELS = {h: hour_label(h) for h in range(24)}
LABEL_TO_HOUR = {v: k for k, v in HOUR_LABELS.items()}

# ─────────────────────────────────────────────────────────────────────────────
# Session state
# ─────────────────────────────────────────────────────────────────────────────
if "resource_assignments" not in st.session_state:
    st.session_state.resource_assignments = deepcopy(DEFAULT_RESOURCES)
if "date_idx" not in st.session_state:
    st.session_state.date_idx = 0
if "last_map_type" not in st.session_state:
    st.session_state.last_map_type = None

# ─────────────────────────────────────────────────────────────────────────────
# Data helpers
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Reading file…")
def load_raw(file_bytes: bytes) -> pd.DataFrame:
    df = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0)
    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    df["Performing Service Resource"] = df["Performing Service Resource"].astype(str).str.strip()
    df["Order Procedure"] = df["Order Procedure"].astype(str).str.strip()

    # Remap: Kappa/Lambda Free Light Chains Panel
    df.loc[
        (df["Order Procedure"] == "Kappa/Lambda Free Light Chains Panel") &
        (df["Performing Service Resource"] == "NCH COBAS"),
        "Performing Service Resource",
    ] = "NCI Manual Flow Bench"

    # Remap: Manual Diff
    df.loc[
        (df["Order Procedure"] == "Manual Diff") &
        (df["Performing Service Resource"] == "Keck HEME Orders"),
        "Performing Service Resource",
    ] = "NCH HEME Orders"

    df["Date/Time - Complete"] = pd.to_datetime(df["Date/Time - Complete"], errors="coerce")
    df = df.dropna(subset=["Date/Time - Complete"])
    df["hour"] = df["Date/Time - Complete"].dt.hour.astype(int)
    df["complete_date"] = df["Date/Time - Complete"].dt.date
    df["Complete Volume"] = (
        pd.to_numeric(df["Complete Volume"], errors="coerce").fillna(0).astype(float)
    )
    return df


def filter_for_map(df: pd.DataFrame, map_type: str) -> pd.DataFrame:
    resources = st.session_state.resource_assignments[map_type]
    out = df[df["Performing Service Resource"].isin(resources)].copy()
    out = out[~out["Order Procedure"].isin(EXCLUDE_PROCS)].copy()
    top30 = (
        out.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(30).index.tolist()
    )
    return out[out["Order Procedure"].isin(top30)].copy()


def build_pivot(df: pd.DataFrame, selected_date, hour_range):
    """
    Returns pivot_display, df_date_hour, df_date, hours
      pivot_display  — labeled columns + Total, sorted by Total desc
      df_date_hour   — raw rows for selected date + hours (for PNG)
      df_date        — raw rows for selected date only (for drill-down)
      hours          — list of int hours shown
    """
    h_start, h_end = hour_range
    hours = list(range(h_start, h_end + 1))

    df_date = df[df["complete_date"] == selected_date].copy()
    df_date_hour = df_date[df_date["hour"].isin(hours)].copy()

    if df_date_hour.empty:
        return None, None, df_date, hours

    pivot = (
        df_date_hour.pivot_table(
            index="Order Procedure",
            columns="hour",
            values="Complete Volume",
            aggfunc="sum",
            fill_value=0.0,
        ).reindex(columns=hours, fill_value=0.0)
    )
    pivot["Total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("Total", ascending=False)
    pivot.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in pivot.columns]
    return pivot, df_date_hour, df_date, hours


def style_pivot(pivot: pd.DataFrame, vmax: int):
    hour_cols = [c for c in pivot.columns if c != "Total"]
    styled = (
        pivot.style
        .background_gradient(cmap="viridis_r", vmin=0, vmax=vmax, subset=hour_cols)
        .format("{:.0f}")
        .set_properties(**{"text-align": "center"})
        .set_properties(subset=["Total"], **{
            "font-weight": "bold",
            "font-size": "13px",
            "border-left": "2px solid #888",
        })
        .set_table_styles([
            {"selector": "th", "props": [("text-align", "center"), ("font-size", "12px")]},
            {"selector": "th.row_heading", "props": [("text-align", "left"), ("min-width", "220px")]},
        ])
    )
    return styled


def build_png(df_date_hour: pd.DataFrame, map_type: str, selected_date, hours: list) -> bytes:
    vmax = VMAX[map_type]
    n_hours = len(hours)

    raw = (
        df_date_hour.pivot_table(
            index="Order Procedure",
            columns="hour",
            values="Complete Volume",
            aggfunc="sum",
            fill_value=0.0,
        ).reindex(columns=hours, fill_value=0.0)
    )
    raw["__total__"] = raw.sum(axis=1)
    raw = raw.sort_values("__total__", ascending=False)
    row_totals = raw["__total__"].values
    raw = raw.drop(columns=["__total__"])

    mat = raw.to_numpy()
    ylabels = raw.index.tolist()
    x_labels = [HOUR_LABELS[h] for h in hours]

    fig_w = max(10, 0.6 * n_hours)
    fig_h = max(5, 0.35 * len(ylabels))
    fig, ax = plt.subplots(figsize=(fig_w, fig_h), dpi=150)

    im = ax.imshow(mat, aspect="auto", cmap="viridis_r", vmin=0, vmax=vmax)

    date_str = pd.Timestamp(selected_date).strftime("%B %d, %Y")
    ax.set_title(f"{map_type} – Top 30 Procedures | {date_str}", fontsize=11, pad=10)

    ax.set_xticks(np.arange(n_hours) + 0.5)
    ax.set_xticklabels(x_labels, rotation=45, ha="right", fontsize=8)
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
            fontsize=8, fontweight="bold", color="black", transform=ax.transData)
    for i, total in enumerate(row_totals):
        ax.text(total_x, i, f"{total:.0f}", ha="center", va="center",
                fontsize=8, fontweight="bold", color="black", transform=ax.transData)
    ax.set_xlim(-0.5, n_hours + 0.25)
    fig.tight_layout()

    buf = io.BytesIO()
    fig.savefig(buf, format="png", dpi=250, bbox_inches="tight")
    buf.seek(0)
    plt.close(fig)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# Sidebar
# ─────────────────────────────────────────────────────────────────────────────
with st.sidebar:
    st.header("Controls")

    map_type = st.selectbox("Map type", list(DEFAULT_RESOURCES.keys()))

    # Reset date index when map type changes
    if st.session_state.last_map_type != map_type:
        st.session_state.date_idx = 0
        st.session_state.last_map_type = map_type

    uploaded = st.file_uploader(
        "Upload Excel file",
        type=["xlsx", "xls"],
        help="First sheet is always used regardless of tab name.",
    )

    st.markdown("---")

    if uploaded:
        file_bytes = uploaded.read()
        raw_df = load_raw(file_bytes)
        filtered_df = filter_for_map(raw_df, map_type)

        available_dates = sorted(filtered_df["complete_date"].unique())
        date_labels = [pd.Timestamp(d).strftime("%B %d, %Y") for d in available_dates]

        if not available_dates:
            st.warning("No data found for this map type.")
            st.stop()

        # Clamp index in case dates changed
        st.session_state.date_idx = max(
            0, min(st.session_state.date_idx, len(available_dates) - 1)
        )

        # Date navigation
        st.markdown("**Date**")
        col_prev, col_next = st.columns(2)
        with col_prev:
            if st.button("◀ Prev", use_container_width=True):
                st.session_state.date_idx = max(0, st.session_state.date_idx - 1)
                st.rerun()
        with col_next:
            if st.button("Next ▶", use_container_width=True):
                st.session_state.date_idx = min(
                    len(available_dates) - 1, st.session_state.date_idx + 1
                )
                st.rerun()

        sel_label = st.selectbox(
            "Select date",
            date_labels,
            index=st.session_state.date_idx,
            label_visibility="collapsed",
        )
        st.session_state.date_idx = date_labels.index(sel_label)
        selected_date = available_dates[st.session_state.date_idx]

        st.caption(f"Day {st.session_state.date_idx + 1} of {len(available_dates)}")

        # Hour range
        st.markdown("---")
        st.markdown("**Hour range**")
        hour_range = st.slider("Hours", 0, 23, (0, 23), label_visibility="collapsed")
        h0, h1 = hour_range

        def fmt_h(h):
            hr12 = 12 if h % 12 == 0 else h % 12
            s = "AM" if h < 12 else "PM"
            return f"{hr12}:00 {s}"

        st.caption(f"{fmt_h(h0)} → {fmt_h(h1)}")

        # ── Resource Allocation ──────────────────────────────────────────────
        st.markdown("---")
        with st.expander("Resource Allocation", expanded=False):
            st.markdown(
                "Reassign instruments between maps. "
                "Each resource should appear in only one map."
            )

            new_assignments = {}
            for mt in DEFAULT_RESOURCES:
                new_assignments[mt] = st.multiselect(
                    mt,
                    options=ALL_RESOURCES,
                    default=st.session_state.resource_assignments.get(mt, []),
                    key=f"res_{mt}",
                )

            # Warn about duplicates
            flat = [r for rs in new_assignments.values() for r in rs]
            dupes = sorted({r for r in flat if flat.count(r) > 1})
            if dupes:
                st.warning(f"⚠️ In multiple maps: {', '.join(dupes)}")

            col_a, col_r = st.columns(2)
            with col_a:
                if st.button("✅ Apply", use_container_width=True, type="primary"):
                    st.session_state.resource_assignments = new_assignments
                    st.cache_data.clear()
                    st.rerun()
            with col_r:
                if st.button("↩ Reset defaults", use_container_width=True):
                    st.session_state.resource_assignments = deepcopy(DEFAULT_RESOURCES)
                    st.cache_data.clear()
                    st.rerun()


# ─────────────────────────────────────────────────────────────────────────────
# Main panel
# ─────────────────────────────────────────────────────────────────────────────
if not uploaded:
    st.markdown("## 👈 Upload a file to get started")
    st.info(
        "Use the sidebar to upload your Excel source file and choose a map type. "
        "The interactive heatmap table will appear here with drill-down support "
        "and a PNG export option."
    )
    st.stop()

pivot, df_date_hour, df_date, hours = build_pivot(filtered_df, selected_date, hour_range)

if pivot is None:
    st.warning("No data found for this date / hour range combination.")
    st.stop()

# ── Header + metrics ─────────────────────────────────────────────────────────
date_str = pd.Timestamp(selected_date).strftime("%B %d, %Y")
st.markdown(f"## {map_type}  ·  {date_str}")

total_vol = int(pivot["Total"].sum())
top_proc = pivot["Total"].idxmax()
hour_cols_only = [c for c in pivot.columns if c != "Total"]
peak_hour = pivot[hour_cols_only].sum().idxmax()
num_procs = len(pivot)

m1, m2, m3, m4 = st.columns(4)
m1.metric("Total Volume", f"{total_vol:,}")
m2.metric("Top Procedure", top_proc)
m3.metric("Peak Hour", peak_hour)
m4.metric("Procedures Shown", num_procs)

st.markdown("---")

# ── Interactive heatmap table ─────────────────────────────────────────────────
st.markdown("### Completed volume per hour")
st.caption(
    f"Color scale: **yellow** = low volume  →  **dark purple** = ≥ {VMAX[map_type]} "
    f"completions/hour. **Total** column shows the full-day sum per procedure."
)

styled = style_pivot(pivot, VMAX[map_type])
table_height = min(80 + 35 * len(pivot), 900)
st.dataframe(styled, use_container_width=True, height=table_height)

# ── Download PNG ──────────────────────────────────────────────────────────────
png_bytes = build_png(df_date_hour, map_type, selected_date, hours)
file_prefix = map_type.replace(" ", "_")
date_tag = pd.Timestamp(selected_date).strftime("%Y-%m-%d")
st.download_button(
    label="Download as PNG",
    data=png_bytes,
    file_name=f"{file_prefix}_Top30_{date_tag}.png",
    mime="image/png",
)

st.markdown("---")

# ── Hourly volume bar chart ───────────────────────────────────────────────────
with st.expander("Hourly volume bar chart", expanded=False):
    hourly = pivot[hour_cols_only].sum().reset_index()
    hourly.columns = ["Hour", "Total Volume"]
    st.bar_chart(hourly.set_index("Hour"), height=220)

# ── Cell drill-down ───────────────────────────────────────────────────────────
with st.expander("Drill into a cell — individual completion times", expanded=False):
    st.markdown(
        "Select a **procedure** and **hour** to see every individual completion "
        "event recorded in that cell. Use the scroll bar if there are many rows."
    )

    dc1, dc2 = st.columns(2)
    with dc1:
        sel_proc = st.selectbox("Procedure", pivot.index.tolist(), key="drill_proc")
    with dc2:
        sel_hour_label = st.selectbox("Hour", hour_cols_only, key="drill_hour")

    sel_hour_int = LABEL_TO_HOUR[sel_hour_label]

    # Filter from df_date (full day, not limited to hour range) so the user
    # can drill into any hour shown in the selector
    detail = df_date[
        (df_date["Order Procedure"] == sel_proc) &
        (df_date["hour"] == sel_hour_int)
    ].copy()

    detail = detail.sort_values("Date/Time - Complete")

    # Clean up display columns
    show_cols = {
        "Date/Time - Complete": "Completed At",
        "Performing Service Resource": "Resource",
        "Complete Volume": "Volume",
    }
    show_cols = {k: v for k, v in show_cols.items() if k in detail.columns}
    detail_display = (
        detail[list(show_cols.keys())]
        .rename(columns=show_cols)
        .reset_index(drop=True)
    )

    if "Completed At" in detail_display.columns:
        detail_display["Completed At"] = pd.to_datetime(
            detail_display["Completed At"]
        ).dt.strftime("%Y-%m-%d  %H:%M:%S")

    if detail_display.empty:
        st.info(
            f"No completions recorded for **{sel_proc}** "
            f"during the **{sel_hour_label}** hour on {date_str}."
        )
    else:
        cell_vol = (
            int(detail_display["Volume"].sum())
            if "Volume" in detail_display.columns
            else len(detail_display)
        )
        st.markdown(
            f"**{len(detail_display)} completion event(s)** · "
            f"*{sel_proc}* · **{sel_hour_label}** hour · "
            f"Total volume in cell: **{cell_vol}**"
        )
        drill_height = min(80 + 35 * len(detail_display), 500)
        st.dataframe(detail_display, use_container_width=True, height=drill_height)
