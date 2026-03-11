"""
app.py — Lab Productivity Heatmap Dashboard
Keck Medicine of USC

Architecture:
  - Master dataset stored as Snappy-compressed Parquet in GitHub (data/lab_data.parquet)
  - On load: reads Parquet from GitHub → renders interactive heatmaps
  - On upload: parses XLSX, merges into master, writes back to GitHub
  - Google Drive credentials exist in secrets but Drive is only used as a
    fallback read/write path when GitHub is not configured
  - After a write, fresh_df in session_state bypasses the GitHub CDN stale-read
    window; user can force a re-fetch with the "Refresh data" button
"""

import base64
import calendar as _cal
import io
import json
import time
from copy import deepcopy
from datetime import date

import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
from matplotlib import font_manager
import requests
import streamlit as st



# ═════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG  (must be the first Streamlit call)
# ═════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Lab Productivity · Keck Medicine",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
)


# ═════════════════════════════════════════════════════════════════════════════
# THEME & GLOBAL CSS
# ═════════════════════════════════════════════════════════════════════════════
_NAVY   = "#6F1828"   # USC Maroon
_GOLD   = "#EDC153"   # USC Gold
_STEEL  = "#57121f"   # Dark USC Maroon
_WHITE  = "#FFFFFF"
_LIGHT  = "#F4F6FA"
_MUTED  = "#6B7280"
_BORDER = "#D1D5DB"

st.markdown("""
<style>
/* ══════════════════════════════════════════════════════════
   BASE — background & font
   Matches config.toml: backgroundColor = "#f4f4f4"
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
   Matches config.toml: secondaryBackgroundColor = "#1a1a1a"
   ══════════════════════════════════════════════════════════ */
[data-testid="stSidebar"] {
    background-color: #1a1a1a !important;
    border-right: 1px solid #2e2e2e !important;
}
/* Section headings (### markdown) */
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3 {
    color: #EDC153 !important;
    font-size: 0.72rem !important;
    font-weight: 700 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    margin-bottom: 0.25rem !important;
    margin-top: 0.1rem !important;
}
/* General text / labels inside sidebar */
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
[data-testid="stSidebar"] label {
    color: #e8e8e8 !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
}
/* Caption / small text */
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] small,
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
    color: #999999 !important;
    font-size: 0.73rem !important;
    font-weight: 400 !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
}
/* Dividers inside sidebar */
[data-testid="stSidebar"] hr {
    border-color: #2e2e2e !important;
    margin: 0.6rem 0 !important;
}

/* ══════════════════════════════════════════════════════════
   BUTTONS — USC Maroon everywhere
   Uses html body prefix for higher specificity than Streamlit's own CSS
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

/* ══════════════════════════════════════════════════════════
   SELECTBOXES / DROPDOWNS — USC Maroon box, white text
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
/* Dropdown option list */
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
   DATE INPUT — USC Maroon box, white text (the input field only)
   ══════════════════════════════════════════════════════════ */
[data-testid="stDateInput"] input,
[data-testid="stSidebar"] [data-testid="stDateInput"] input {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border: 1px solid #57121f !important;
    border-radius: 5px !important;
}
/* Only the calendar-toggle icon (inside the input row), not the popup */
[data-testid="stDateInput"] > div > div > div > svg {
    fill: #ffffff !important;
    color: #ffffff !important;
}

/* ══════════════════════════════════════════════════════════
   DATE PICKER POPUP (BaseWeb calendar portal)
   Ensure month/year header, nav arrows and day labels are
   clearly readable on the popup's white background.
   ══════════════════════════════════════════════════════════ */
[data-baseweb="calendar"] {
    background-color: #ffffff !important;
    color: #0f172a !important;
}
/* Month / year header buttons and navigation arrows */
[data-baseweb="calendar"] button {
    color: #0f172a !important;
    background-color: transparent !important;
}
[data-baseweb="calendar"] button:hover {
    background-color: #f0f0f0 !important;
}
/* All SVGs inside the popup (prev/next chevrons, etc.) */
[data-baseweb="calendar"] svg {
    fill: #0f172a !important;
    color: #0f172a !important;
}
/* Day-of-week header row */
[data-baseweb="calendar"] [data-testid="calendar-day-label"],
[data-baseweb="calendar"] abbr {
    color: #64748b !important;
}
/* Individual day cells */
[data-baseweb="calendar"] [role="gridcell"] button,
[data-baseweb="calendar"] [data-testid="calendar-day"] {
    color: #0f172a !important;
    background-color: transparent !important;
}
/* Selected day highlight */
[data-baseweb="calendar"] [aria-selected="true"] button,
[data-baseweb="calendar"] [data-selected="true"] {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border-radius: 50% !important;
}
/* Today marker */
[data-baseweb="calendar"] [data-today="true"] button {
    border: 2px solid #6F1828 !important;
    border-radius: 50% !important;
}
/* Disabled / out-of-range days */
[data-baseweb="calendar"] [aria-disabled="true"] button {
    color: #cccccc !important;
}

/* ══════════════════════════════════════════════════════════
   RADIO BUTTONS — transparent, light text on dark sidebar
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
   TEXT INPUTS — white background, dark text (password fields)
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
   METRIC CARDS (native st.metric)
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

/* ══════════════════════════════════════════════════════════
   DIVIDERS (main panel)
   ══════════════════════════════════════════════════════════ */
hr {
    border-color: #e2e8f0 !important;
    margin: 1.1rem 0 !important;
}

/* ══════════════════════════════════════════════════════════
   CUSTOM COMPONENT CLASSES
   USC Maroon #6F1828, USC Gold #EDC153, textColor #0f172a
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
</style>
""", unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# APP-LEVEL PASSWORD GATE
# Runs before any content renders. Gate is skipped if app_password is not set
# in st.secrets, so local dev and open deployments work without configuration.
# ═════════════════════════════════════════════════════════════════════════════
_app_password = st.secrets.get("app_password", None)

if "app_authenticated" not in st.session_state:
    st.session_state["app_authenticated"] = False

if _app_password is not None and not st.session_state["app_authenticated"]:
    # Centre the card vertically on a desktop viewport
    st.markdown('<div style="height: 15vh; min-height: 48px;"></div>',
                unsafe_allow_html=True)
    _, col, _ = st.columns([1, 0.9, 1])
    with col:
        st.markdown("""
            <div style="
                background: linear-gradient(150deg, #6F1828 0%, #521322 60%, #3d0e19 100%);
                padding: 2rem 2.4rem 1.8rem 2.4rem;
                border-radius: 12px 12px 0 0;
                text-align: center;
                box-shadow: none;
            ">
                <div style="
                    display: inline-block;
                    background: rgba(237,193,83,0.18);
                    border: 1px solid rgba(237,193,83,0.4);
                    color: #EDC153;
                    font-family: 'Inter', system-ui, sans-serif;
                    font-size: 0.62rem;
                    font-weight: 700;
                    letter-spacing: 0.18em;
                    text-transform: uppercase;
                    padding: 3px 12px;
                    border-radius: 20px;
                    margin-bottom: 1rem;
                ">Keck Medicine of USC</div>
                <div style="
                    color: #ffffff;
                    font-family: 'Inter', system-ui, sans-serif;
                    font-size: 1.45rem;
                    font-weight: 700;
                    letter-spacing: 0.01em;
                    margin: 0 0 0.3rem 0;
                    line-height: 1.2;
                ">Laboratory Productivity</div>
                <div style="
                    color: rgba(237,193,83,0.85);
                    font-family: 'Inter', system-ui, sans-serif;
                    font-size: 0.82rem;
                    font-weight: 400;
                    letter-spacing: 0.01em;
                    margin: 0;
                ">Analytics Dashboard</div>
            </div>
            <div style="
                background: #ffffff;
                padding: 1.8rem 2.4rem 2.2rem 2.4rem;
                border-radius: 0 0 12px 12px;
                border: 1px solid #dde1e7;
                border-top: none;
                box-shadow: 0 8px 32px rgba(0,0,0,0.12);
            ">
                <p style="
                    color: #475569;
                    font-size: 0.82rem;
                    margin: 0 0 1.2rem 0;
                    font-family: 'Inter', system-ui, sans-serif;
                ">Enter your access password to continue.</p>
        """, unsafe_allow_html=True)
        with st.form("login_form", enter_to_submit=True):
            password = st.text_input("Password", type="password",
                                     label_visibility="collapsed",
                                     placeholder="Password")
            submitted = st.form_submit_button("Sign In",
                                              use_container_width=True)
            if submitted:
                if password == st.secrets.get("app_password", ""):
                    st.session_state["app_authenticated"] = True
                    st.rerun()
                else:
                    st.error("Incorrect password. Please try again.")
        st.markdown("""
            </div>
            <div style="text-align:center; margin-top: 1.2rem; color: #94a3b8; font-size: 0.7rem; font-family: 'Inter', system-ui, sans-serif;">
                Keck Medicine of USC &nbsp;·&nbsp; Laboratory Analytics
            </div>
        """, unsafe_allow_html=True)
    st.stop()


# ═════════════════════════════════════════════════════════════════════════════
# MATPLOTLIB FONT
# ═════════════════════════════════════════════════════════════════════════════
def _set_mpl_font() -> None:
    """Use Palatino if available, fall back to generic serif."""
    installed = {f.name for f in font_manager.fontManager.ttflist}
    for name in ("Palatino Linotype", "Palatino"):
        if name in installed:
            mpl.rcParams["font.family"] = name
            return
    mpl.rcParams["font.family"] = "serif"

_set_mpl_font()


# ═════════════════════════════════════════════════════════════════════════════
# CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════
# All per-site configuration lives here.  To add a new lab site, add one entry
# to SITE_CONFIG with its resource list and heatmap colour-scale ceiling.

SITE_CONFIG: dict[str, dict] = {
    "Keck Core": {
        "resources": [
            "Keck Abbott DI", "Keck Coagulation", "Keck Cobas",
            "Keck HEME Orders", "Keck IRIS", "Keck ISED", "Keck SmartLyte A",
            "Keck TEG 5000", "Keck Urinalysis", "USC Manual Coagulation Bench",
            "USC Manual Hematology Bench", "USC Manual Urinalysis Bench",
            "USC Serology Routine Bench",
        ],
        "vmax": 50,
    },
    "Norris Core": {
        "resources": [
            "NCH Coagulation", "NCH COBAS", "NCH HEME Orders", "NCH IRIS",
            "NCI Manual Chemistry Bench", "NCI Manual Hematology Bench",
            "NCI Stem Cell Bench", "NCH Cobas PRO A", "NCH Cobas PRO B",
            "NCH GEM 4000 H", "NCH GEM 4000 I",
        ],
        "vmax": 30,
    },
    "Norris Specialty": {
        "resources": [
            "NCH DS2 A", "NCH HydraSys", "NCH PFA 100", "NCH Tosoh G8",
            "NCI Manual Flow Bench", "NCI Manual Verify Now Bench",
        ],
        "vmax": 20,
    },
}

# Derived helpers — computed once at import from SITE_CONFIG
DEFAULT_RESOURCES: dict[str, list] = {k: v["resources"] for k, v in SITE_CONFIG.items()}
VMAX:              dict[str, int]  = {k: v["vmax"]      for k, v in SITE_CONFIG.items()}
ALL_RESOURCES:     list[str]       = sorted({r for v in SITE_CONFIG.values() for r in v["resources"]})
MAP_TYPES:         list[str]       = list(SITE_CONFIG.keys())

# Procedures always excluded from heatmaps (regardless of site)
EXCLUDE_PROCS: set[str] = {
    "Glomerular Filtration Rate Estimated",
    ".Diff Auto -",
    "Manual Diff-",
}

# Resource remaps applied at parse time.
# Format: { (order_procedure, old_resource): new_resource }
# To add a remap, append one entry here — no other code needs to change.
RESOURCE_REMAPS: dict[tuple[str, str], str] = {
    ("Kappa/Lambda Free Light Chains Panel", "NCH COBAS"):    "NCI Manual Flow Bench",
    ("Manual Diff",                          "Keck HEME Orders"): "NCH HEME Orders",
}

# Column names required from source files
REQUIRED_COLS: set[str] = {
    "Performing Service Resource",
    "Order Procedure",
    "Date/Time - Complete",
    "Complete Volume",
}

# Parquet filename (used for both Drive and GitHub paths)
PARQUET_FILENAME = "lab_data.parquet"

# Hour display helpers
def _hour_label(h: int) -> str:
    hr12   = 12 if h % 12 == 0 else h % 12
    suffix = "AM" if h < 12 else "PM"
    return f"{hr12}{suffix}"

HOUR_LABELS   = {h: _hour_label(h) for h in range(24)}
LABEL_TO_HOUR = {v: k for k, v in HOUR_LABELS.items()}

# Storage backend flags (evaluated once)
GITHUB_CONFIGURED = "github" in st.secrets
DRIVE_CONFIGURED  = (
    "gcp_service_account" in st.secrets and "google_drive" in st.secrets
)


# ═════════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALISATION
# ═════════════════════════════════════════════════════════════════════════════
_ss = st.session_state

if "resource_assignments" not in _ss:
    _ss.resource_assignments = deepcopy(DEFAULT_RESOURCES)
if "last_map_type" not in _ss:
    _ss.last_map_type = None


# ═════════════════════════════════════════════════════════════════════════════
# GOOGLE DRIVE HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _get_drive_creds():
    """Build and immediately refresh Drive service-account credentials.

    Always called fresh for write operations; for read operations the Drive
    service handle is cached via ``_get_drive_service()``.
    """
    from google.auth.transport.requests import Request as _GReq
    info = dict(st.secrets["gcp_service_account"])
    pk = info.get("private_key", "")
    if "\\n" in pk:
        pk = pk.replace("\\n", "\n")
    info["private_key"] = pk
    creds = service_account.Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/drive"]
    )
    creds.refresh(_GReq())
    return creds


@st.cache_resource(show_spinner=False)
def _get_drive_service():
    """Cached Google Drive API service handle.

    Note: the underlying token is refreshed on first call only.  For
    long-lived sessions this handle may eventually expire — in practice
    Streamlit Cloud restarts apps frequently enough that this is not an
    issue.  Write operations bypass this via ``_get_authed_session()``.
    """
    return build("drive", "v3", credentials=_get_drive_creds(), cache_discovery=False)


def _get_authed_session():
    """Return a freshly authorised requests.Session for Drive write operations."""
    from google.auth.transport.requests import AuthorizedSession
    return AuthorizedSession(_get_drive_creds())


def _get_drive_file_meta(folder_id: str) -> dict | None:
    """Return the Drive metadata dict for lab_data.parquet, or None if absent."""
    svc = _get_drive_service()
    res = svc.files().list(
        q=f"'{folder_id}' in parents and name='{PARQUET_FILENAME}' and trashed=false",
        fields="files(id, name, modifiedTime)",
        supportsAllDrives=True,
        includeItemsFromAllDrives=True,
    ).execute()
    files = res.get("files", [])
    return files[0] if files else None


def _download_drive_file(file_id: str, retries: int = 4) -> bytes:
    """Download a Drive file, retrying with exponential back-off on errors."""
    last_err = None
    for attempt in range(retries):
        try:
            svc = _get_drive_service()
            req = svc.files().get_media(fileId=file_id, supportsAllDrives=True)
            buf = io.BytesIO()
            dl  = MediaIoBaseDownload(buf, req, chunksize=4 * 1024 * 1024)
            done = False
            while not done:
                _, done = dl.next_chunk()
            buf.seek(0)
            return buf.read()
        except Exception as exc:
            last_err = exc
            time.sleep(2 ** attempt)
    raise RuntimeError(f"Drive download failed after {retries} attempts: {last_err}")


def _upload_to_drive(folder_id: str, parquet_bytes: bytes) -> None:
    """Create or overwrite lab_data.parquet in the specified Drive folder.

    Uses an AuthorizedSession (always fresh token) and the multipart upload
    endpoint so metadata and content are sent in a single request.
    """
    session = _get_authed_session()

    # Check whether the file already exists
    resp = session.get(
        "https://www.googleapis.com/drive/v3/files",
        params={
            "q": f"'{folder_id}' in parents and name='{PARQUET_FILENAME}' and trashed=false",
            "fields": "files(id)",
            "supportsAllDrives": "true",
            "includeItemsFromAllDrives": "true",
        },
        timeout=30,
    )
    resp.raise_for_status()
    existing = resp.json().get("files", [])

    boundary  = "lab_heatmap_boundary"
    meta_json = json.dumps(
        {"name": PARQUET_FILENAME} if existing
        else {"name": PARQUET_FILENAME, "parents": [folder_id]}
    ).encode()
    sep  = b"\r\n"
    body = (
        b"--" + boundary.encode() + sep
        + b"Content-Type: application/json; charset=UTF-8" + sep + sep
        + meta_json + sep
        + b"--" + boundary.encode() + sep
        + b"Content-Type: application/octet-stream" + sep + sep
        + parquet_bytes + sep
        + b"--" + boundary.encode() + b"--" + sep
    )
    headers = {"Content-Type": f"multipart/related; boundary={boundary}"}

    if existing:
        r = session.patch(
            f"https://www.googleapis.com/upload/drive/v3/files/{existing[0]['id']}",
            params={"uploadType": "multipart", "supportsAllDrives": "true"},
            headers=headers, data=body, timeout=120,
        )
    else:
        r = session.post(
            "https://www.googleapis.com/upload/drive/v3/files",
            params={"uploadType": "multipart", "supportsAllDrives": "true"},
            headers=headers, data=body, timeout=120,
        )
    if not r.ok:
        raise RuntimeError(f"Drive upload failed {r.status_code}: {r.text[:500]}")


# ═════════════════════════════════════════════════════════════════════════════
# GITHUB STORAGE HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _gh_headers() -> dict:
    """Return authenticated GitHub API request headers."""
    return {
        "Authorization": f"Bearer {st.secrets['github']['token']}",
        "Accept": "application/vnd.github+json",
    }


def _gh_coords() -> tuple[str, str, str]:
    """Return (owner, repo_name, file_path) from Streamlit secrets."""
    repo  = st.secrets["github"]["repo"]
    path  = st.secrets["github"].get("data_path", "data/lab_data.parquet")
    owner, repo_name = repo.split("/", 1)
    return owner, repo_name, path


@st.cache_data(show_spinner=False, ttl=60)
def _get_github_sha() -> str | None:
    """Return the current blob SHA of the master parquet on GitHub, or None if absent.

    Cached for 60 s.  Always cleared explicitly after any write operation so
    the next read picks up the new SHA immediately.
    """
    owner, repo, path = _gh_coords()
    resp = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
        headers=_gh_headers(), timeout=15,
    )
    if resp.status_code == 404:
        return None
    resp.raise_for_status()
    return resp.json()["sha"]


def _read_parquet_from_github(retries: int = 3) -> bytes:
    """Download the master parquet from GitHub with automatic retry.

    Handles both small files (base64-encoded inline in the Contents API
    response) and large files (GitHub returns an empty 'content' field with a
    separate 'download_url' for files > 1 MB).
    """
    owner, repo, path = _gh_coords()
    last_err = None
    for attempt in range(retries):
        try:
            resp = requests.get(
                f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
                headers=_gh_headers(), timeout=30,
            )
            resp.raise_for_status()
            data        = resp.json()
            content_str = data.get("content", "").strip()

            if content_str:
                raw = base64.b64decode(content_str)
            elif data.get("download_url"):
                dl  = requests.get(data["download_url"], headers=_gh_headers(), timeout=60)
                dl.raise_for_status()
                raw = dl.content
            else:
                raw = b""

            if len(raw) == 0:
                raise ValueError(
                    "Parquet file on GitHub is 0 bytes. "
                    "Use Data Management → Upload to re-populate the master dataset."
                )
            return raw

        except Exception as exc:
            last_err = exc
            if attempt < retries - 1:
                time.sleep(2 ** attempt)

    raise RuntimeError(f"GitHub read failed after {retries} attempts: {last_err}")


def _write_parquet_to_github(parquet_bytes: bytes) -> None:
    """Create or update the master parquet file on GitHub.

    Validates the payload size before touching the remote file to protect
    against accidentally overwriting good data with an empty or corrupt file.
    Clears the SHA cache on success so future reads use the new SHA.
    """
    if len(parquet_bytes) < 100:
        raise ValueError(
            f"Refusing to write {len(parquet_bytes)}-byte payload — data appears empty."
        )

    owner, repo, path = _gh_coords()

    # Confirm the repo is reachable before attempting the write
    check = requests.get(
        f"https://api.github.com/repos/{owner}/{repo}",
        headers=_gh_headers(), timeout=15,
    )
    if check.status_code == 404:
        raise RuntimeError(
            f"GitHub repo '{owner}/{repo}' not found.  "
            "Check that the 'repo' value in your Streamlit secrets exactly matches "
            "your GitHub repo name (it is case-sensitive)."
        )
    check.raise_for_status()

    sha     = _get_github_sha()
    payload = {
        "message": "Update lab_data.parquet via dashboard",
        "content": base64.b64encode(parquet_bytes).decode(),
    }
    if sha:
        payload["sha"] = sha

    resp = requests.put(
        f"https://api.github.com/repos/{owner}/{repo}/contents/{path.lstrip('/')}",
        headers=_gh_headers(), json=payload, timeout=60,
    )
    if not resp.ok:
        raise RuntimeError(
            f"GitHub write failed {resp.status_code}:\n"
            f"Repo: {owner}/{repo}  Path: {path}\n"
            f"{resp.text[:300]}"
        )
    _get_github_sha.clear()


# ═════════════════════════════════════════════════════════════════════════════
# DATA PARSING & MERGING
# ═════════════════════════════════════════════════════════════════════════════

def parse_single_file(file_bytes: bytes, filename: str = "") -> pd.DataFrame:
    """Parse a CSV or Excel export into a clean, analysis-ready DataFrame.

    Only the four columns in ``REQUIRED_COLS`` are loaded for speed.
    Resource remaps from ``RESOURCE_REMAPS`` are applied before returning.
    Adds derived ``hour`` (int) and ``complete_date`` (datetime.date) columns.
    """
    fname = filename.lower()
    if fname.endswith(".csv") or fname.endswith(".txt"):
        hdr      = pd.read_csv(io.BytesIO(file_bytes), nrows=0)
        hdr.columns = [c.strip() for c in hdr.columns]
        use_cols = [c for c in hdr.columns if c in REQUIRED_COLS]
        df = pd.read_csv(io.BytesIO(file_bytes), usecols=use_cols, low_memory=False)
    else:
        hdr      = pd.read_excel(io.BytesIO(file_bytes), sheet_name=0, nrows=0)
        hdr.columns = [c.strip() if isinstance(c, str) else c for c in hdr.columns]
        use_cols = [c for c in hdr.columns if c in REQUIRED_COLS]
        df = pd.read_excel(
            io.BytesIO(file_bytes), sheet_name=0, usecols=use_cols, engine="openpyxl"
        )

    df.columns = [c.strip() if isinstance(c, str) else c for c in df.columns]
    df["Performing Service Resource"] = df["Performing Service Resource"].astype(str).str.strip()
    df["Order Procedure"]             = df["Order Procedure"].astype(str).str.strip()

    # Apply resource remaps defined in RESOURCE_REMAPS
    for (proc, old_res), new_res in RESOURCE_REMAPS.items():
        mask = (df["Order Procedure"] == proc) & (df["Performing Service Resource"] == old_res)
        df.loc[mask, "Performing Service Resource"] = new_res

    df["Date/Time - Complete"] = pd.to_datetime(df["Date/Time - Complete"], errors="coerce")
    df = df.dropna(subset=["Date/Time - Complete"])
    df["hour"]          = df["Date/Time - Complete"].dt.hour.astype(int)
    df["complete_date"] = df["Date/Time - Complete"].dt.date
    df["Complete Volume"] = (
        pd.to_numeric(df["Complete Volume"], errors="coerce").fillna(0).astype(float)
    )
    return df


def deduplicate_and_merge(
    frames: list[tuple[str, pd.DataFrame]],
) -> tuple[pd.DataFrame, list[dict]]:
    """Merge DataFrames from multiple files, trimming overlapping time windows.

    Strategy:
      1. Sort files by their earliest timestamp.
      2. For each consecutive pair where file[i] overlaps file[i+1], trim
         file[i] at file[i+1]'s start time.  Rows in the overlap window are
         taken exclusively from the later (presumably more complete) file.
      3. The last file is always kept in full.

    Returns (merged_df, summary_records).
    """
    if not frames:
        return pd.DataFrame(), []

    records = sorted(
        [
            {
                "fname":  name,
                "min_dt": df["Date/Time - Complete"].min(),
                "max_dt": df["Date/Time - Complete"].max(),
                "df":     df,
            }
            for name, df in frames
        ],
        key=lambda r: r["min_dt"],
    )

    summary    = []
    result_dfs = []
    for i, rec in enumerate(records):
        df     = rec["df"].copy()
        cutoff = rec["max_dt"]
        if i + 1 < len(records):
            next_min = records[i + 1]["min_dt"]
            if next_min <= cutoff:
                cutoff = next_min
                df = df[df["Date/Time - Complete"] < cutoff]
        result_dfs.append(df)
        summary.append({
            "File":      rec["fname"],
            "Data from": rec["min_dt"].strftime("%Y-%m-%d %H:%M"),
            "Data to":   cutoff.strftime("%Y-%m-%d %H:%M"),
            "Rows kept": len(df),
        })

    return pd.concat(result_dfs, ignore_index=True), summary


def merge_new_into_master(
    existing_df: pd.DataFrame, new_df: pd.DataFrame
) -> tuple[pd.DataFrame, dict]:
    """Merge a newly uploaded file into the existing master dataset.

    Returns (merged_df, stats_dict).  Stats include row counts before/after.
    """
    merged, _ = deduplicate_and_merge([("existing", existing_df), ("new file", new_df)])
    stats = {
        "rows_before":    len(existing_df),
        "rows_after":     len(merged),
        "rows_added":     len(merged) - len(existing_df),
        "new_date_range": f"{new_df['complete_date'].min()} → {new_df['complete_date'].max()}",
    }
    return merged, stats


def df_to_parquet_bytes(df: pd.DataFrame) -> bytes:
    """Serialise a DataFrame to Snappy-compressed Parquet and return raw bytes."""
    buf = io.BytesIO()
    df.to_parquet(buf, index=False, compression="snappy")
    return buf.getvalue()


def _parquet_bytes_to_df(raw: bytes) -> pd.DataFrame:
    """Deserialise Parquet bytes, ensuring derived helper columns exist."""
    df = pd.read_parquet(io.BytesIO(raw))
    if "complete_date" not in df.columns:
        df["complete_date"] = pd.to_datetime(df["Date/Time - Complete"]).dt.date
    if "hour" not in df.columns:
        df["hour"] = pd.to_datetime(df["Date/Time - Complete"]).dt.hour.astype(int)
    return df


def _remove_date_range(df: pd.DataFrame, start: date, end: date) -> pd.DataFrame:
    """Return a copy of df with all rows in [start, end] (inclusive) removed."""
    mask = (df["complete_date"] >= start) & (df["complete_date"] <= end)
    return df[~mask].reset_index(drop=True)


# ═════════════════════════════════════════════════════════════════════════════
# DATA LOADING  (cached; cache-key encodes the file version)
# ═════════════════════════════════════════════════════════════════════════════

@st.cache_data(show_spinner="Loading master dataset…", ttl=300)
def _load_master_data(cache_key: str) -> pd.DataFrame:
    """Load the master parquet from GitHub or Drive.

    ``cache_key`` encodes the current file version (blob SHA for GitHub,
    modifiedTime for Drive) so the Streamlit cache is automatically busted
    when the remote file changes.
    """
    if GITHUB_CONFIGURED:
        return _parquet_bytes_to_df(_read_parquet_from_github())

    folder_id = st.secrets["google_drive"]["folder_id"]
    meta      = _get_drive_file_meta(folder_id)
    if meta is None:
        raise FileNotFoundError("No data file found on Drive. Use Data Management to upload.")
    return _parquet_bytes_to_df(_download_drive_file(meta["id"]))


@st.cache_data(show_spinner="Processing uploaded files…")
def _load_from_uploads(files_bytes: tuple[tuple[str, bytes], ...]) -> tuple[pd.DataFrame, list]:
    """Parse and merge one or more locally uploaded files (no remote storage).

    ``files_bytes`` must be a tuple (not a list) so Streamlit can hash it.
    """
    frames = [(name, parse_single_file(b, filename=name)) for name, b in files_bytes]
    return deduplicate_and_merge(frames)


# ═════════════════════════════════════════════════════════════════════════════
# MAP FILTERING & PIVOT
# ═════════════════════════════════════════════════════════════════════════════

def filter_for_map(df: pd.DataFrame, map_type: str) -> pd.DataFrame:
    """Filter the master dataset to the resources and procedures for one map.

    Applies the current resource assignments from session state and excludes
    globally excluded procedures.  Top-30 is NOT applied here — it is computed
    per-day inside build_pivot (daily view) and per-month inside
    build_monthly_pivot (monthly view) so the ranking reflects the period
    actually being displayed rather than all-time totals.
    """
    resources = _ss.resource_assignments[map_type]
    out = df[df["Performing Service Resource"].isin(resources)].copy()
    out = out[~out["Order Procedure"].isin(EXCLUDE_PROCS)]
    return out


def build_pivot(
    df: pd.DataFrame,
    selected_date: date,
    hour_range: tuple[int, int],
) -> tuple[pd.DataFrame | None, pd.DataFrame | None, pd.DataFrame, list[int]]:
    """Build the procedure × hour pivot table for a given date and hour window.

    Top-30 procedures are ranked by their full-day Complete Volume (all hours),
    then the pivot is built from only those 30 using the selected hour window.
    This ensures the displayed count always reflects the busiest procedures for
    that specific day, not across all historical data.

    Returns (pivot_df, date_hour_df, date_df, hours_list).
    ``pivot_df`` is None when there is no data for this date/hour combination.
    """
    h_start, h_end = hour_range
    hours   = list(range(h_start, h_end + 1))
    df_date = df[df["complete_date"] == selected_date].copy()
    df_dh   = df_date[df_date["hour"].isin(hours)].copy()

    if df_dh.empty:
        return None, None, df_date, hours

    # Rank by full-day volume so the top-30 is not skewed by the hour filter
    top30 = (
        df_date.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(30).index.tolist()
    )
    df_dh = df_dh[df_dh["Order Procedure"].isin(top30)].copy()

    if df_dh.empty:
        return None, None, df_date, hours

    pivot = (
        df_dh.pivot_table(
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
    return pivot, df_dh, df_date, hours


def style_pivot(pivot: pd.DataFrame, vmax: int):
    """Apply viridis_r background-gradient styling to the pivot DataFrame."""
    hour_cols = [c for c in pivot.columns if c != "Total"]
    return (
        pivot.style
        .background_gradient(cmap="viridis_r", vmin=0, vmax=vmax, subset=hour_cols)
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



# ═════════════════════════════════════════════════════════════════════════════
# MONTHLY PIVOT
# ═════════════════════════════════════════════════════════════════════════════

def build_monthly_pivot(
    df: pd.DataFrame,
    year: int,
    month: int,
) -> tuple["pd.DataFrame | None", int, "pd.DataFrame"]:
    """Build a procedure x hour pivot of daily-average volumes for a calendar month.

    Returns (avg_pivot, n_days, month_df).
    avg_pivot is None when there is no data for the selected month.
    """
    month_start = date(year, month, 1)
    month_end   = date(year, month, _cal.monthrange(year, month)[1])
    month_df    = df[
        (df["complete_date"] >= month_start) &
        (df["complete_date"] <= month_end)
    ].copy()

    if month_df.empty:
        return None, 0, month_df

    # Top-30 by total Complete Volume within this month
    top30 = (
        month_df.groupby("Order Procedure")["Complete Volume"]
        .sum().sort_values(ascending=False).head(30).index.tolist()
    )
    month_df = month_df[month_df["Order Procedure"].isin(top30)].copy()

    # Pivot: rows = procedures, columns = hours 0-23
    pivot = (
        month_df.pivot_table(
            index="Order Procedure",
            columns="hour",
            values="Complete Volume",
            aggfunc="sum",
            fill_value=0,
        ).reindex(columns=list(range(24)), fill_value=0)
    )

    n_days = int(month_df["complete_date"].nunique())

    # Divide by n_days to get daily averages
    avg = pivot / n_days
    avg["Total"] = avg.sum(axis=1)
    avg = avg.sort_values("Total", ascending=False)

    # Rename hour columns to labels
    avg.columns = [HOUR_LABELS[c] if isinstance(c, int) else c for c in avg.columns]

    return avg, n_days, month_df


def style_monthly_pivot(pivot: pd.DataFrame, vmax: int):
    """Apply viridis_r gradient styling to a monthly average pivot (1 dp)."""
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
) -> bytes:
    """Render the heatmap as a high-DPI PNG and return raw PNG bytes.

    Only called when the user explicitly requests a download to avoid
    running the expensive matplotlib rendering on every page load.
    """
    vmax    = VMAX[map_type]
    n_hours = len(hours)

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

    im = ax.imshow(mat, aspect="auto", cmap="viridis_r", vmin=0, vmax=vmax)
    ax.set_title(
        f"{map_type} – Top 30 Procedures  |  "
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


# ═════════════════════════════════════════════════════════════════════════════
# UI HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _metric_card(label: str, value: str, sub: str = "", accent: bool = False) -> str:
    """Return an HTML string for a styled metric card."""
    cls  = "metric-card accent" if accent else "metric-card"
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    return (
        f'<div class="{cls}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>'
        f'{sub_html}'
        f'</div>'
    )


def _render_header(map_type: str, date_str: str) -> None:
    """Render the branded Keck Medicine header banner."""
    st.markdown(f"""
    <div class="keck-header">
      <div>
        <h1>Lab Productivity Dashboard</h1>
        <p class="subtitle">{map_type}</p>
      </div>
      <div style="text-align:right;">
        <span class="keck-badge">Laboratory Analytics</span>
        <span class="keck-date-label">{date_str}</span>
      </div>
    </div>
    """, unsafe_allow_html=True)


def _status_chip(text: str, level: str = "ok") -> None:
    """Render a small coloured status chip (level: 'ok', 'warn', or 'error')."""
    cls = {"ok": "status-chip", "warn": "status-chip warn", "error": "status-chip error"}
    st.markdown(f'<div class="{cls.get(level, "status-chip")}">{text}</div>',
                unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# SIDEBAR
# ═════════════════════════════════════════════════════════════════════════════
with st.sidebar:

    # ── Map type selector ────────────────────────────────────────────────────
    st.markdown("### Map Type")
    map_type = st.selectbox("Map type", MAP_TYPES, label_visibility="collapsed")

    # When the map changes, reset the date picker to the most-recent date
    if _ss.last_map_type != map_type:
        _ss.pop("date_picker", None)
        _ss.last_map_type = map_type

    # ── View mode toggle ─────────────────────────────────────────────────────
    st.markdown("### View")
    view_mode = st.radio(
        "View", ["Daily", "Monthly"],
        horizontal=True, label_visibility="collapsed",
    )

    # ── Pending action: reset entire dataset ────────────────────────────────
    if _ss.pop("pending_reset", False):
        try:
            if GITHUB_CONFIGURED:
                owner, repo, path = _gh_coords()
                sha = _get_github_sha()
                if sha:
                    requests.delete(
                        f"https://api.github.com/repos/{owner}/{repo}/contents/{path}",
                        headers=_gh_headers(),
                        json={"message": "Reset master dataset", "sha": sha},
                        timeout=15,
                    )
                    _get_github_sha.clear()
                st.cache_data.clear()
                _ss.pop("fresh_df", None)
                st.success("Master dataset cleared.")
            elif DRIVE_CONFIGURED:
                folder_id_rst = st.secrets["google_drive"]["folder_id"]
                meta_rst      = _get_drive_file_meta(folder_id_rst)
                if meta_rst:
                    _get_drive_service().files().delete(fileId=meta_rst["id"]).execute()
                st.cache_data.clear()
                _ss.pop("fresh_df", None)
                st.success("Master dataset cleared.")
        except Exception as _rst_err:
            st.error(f"Reset failed: {_rst_err}")

    # ── Pending action: delete a date range ─────────────────────────────────
    if "pending_delete_range" in _ss:
        del_info = _ss.pop("pending_delete_range")
        try:
            # Load current master (prefer in-memory snapshot)
            if "fresh_df" in _ss:
                src_df = _ss["fresh_df"]
            elif GITHUB_CONFIGURED:
                sha_dr = _get_github_sha()
                if not sha_dr:
                    raise ValueError("No data on GitHub to delete from.")
                src_df = _parquet_bytes_to_df(_read_parquet_from_github())
            else:
                folder_id_dr = st.secrets["google_drive"]["folder_id"]
                meta_dr      = _get_drive_file_meta(folder_id_dr)
                if not meta_dr:
                    raise ValueError("No data on Drive to delete from.")
                src_df = _parquet_bytes_to_df(_download_drive_file(meta_dr["id"]))

            rows_before = len(src_df)
            new_df      = _remove_date_range(src_df, del_info["start"], del_info["end"])
            pq_bytes    = df_to_parquet_bytes(new_df)

            if GITHUB_CONFIGURED:
                _write_parquet_to_github(pq_bytes)
            else:
                _upload_to_drive(st.secrets["google_drive"]["folder_id"], pq_bytes)

            _ss["fresh_df"] = new_df
            st.success(
                f"Deleted {rows_before - len(new_df):,} rows "
                f"({del_info['start']} → {del_info['end']})."
            )
        except Exception as _dr_err:
            st.error(f"Delete failed: {_dr_err}")

    # ── Data source & loading ────────────────────────────────────────────────
    st.markdown("---")
    st.markdown("### Data Source")

    raw_df    = None
    merge_log = []

    if GITHUB_CONFIGURED or DRIVE_CONFIGURED:
        source_label = "GitHub" if GITHUB_CONFIGURED else "Google Drive"
        st.caption(f"Storage: {source_label}")

        _data_exists = False
        try:
            if "fresh_df" in _ss:
                # Use the in-memory post-upload snapshot — avoids GitHub CDN
                # stale-read delay (can persist for 30–60 s after a commit).
                # The snapshot remains until the user clicks "Refresh data".
                raw_df       = _ss["fresh_df"]
                _data_exists = True

            elif GITHUB_CONFIGURED:
                _sha         = _get_github_sha()
                _data_exists = _sha is not None
                if _data_exists:
                    raw_df = _load_master_data(_sha)
                else:
                    _status_chip("No data yet — upload below", level="warn")

            else:
                _folder_id   = st.secrets["google_drive"]["folder_id"]
                _meta        = _get_drive_file_meta(_folder_id)
                _data_exists = _meta is not None
                if _data_exists:
                    raw_df = _load_master_data(_meta["modifiedTime"])
                else:
                    _status_chip("No data yet — upload below", level="warn")

            if raw_df is not None and not raw_df.empty:
                _chip_text = (
                    f"{len(raw_df):,} rows · "
                    f"{raw_df['complete_date'].min()} → {raw_df['complete_date'].max()}"
                )
                _status_chip(_chip_text, level="ok")

        except Exception as _load_err:
            _status_chip("Load error", level="error")
            st.error(f"Could not load data: {_load_err}")

        # ── Data Management expander ─────────────────────────────────────────
        st.markdown("---")
        with st.expander("Data Management", expanded=not _data_exists):

            # Optional admin password gate
            # Once the correct password is entered, store in session state so
            # the input field is hidden for the rest of the session.
            admin_pw   = st.secrets.get("admin_password", None)
            authorized = True
            if admin_pw:
                if _ss.get("admin_authorized", False):
                    authorized = True
                else:
                    entered_pw = st.text_input(
                        "Admin password", type="password", key="admin_pw"
                    )
                    if entered_pw == admin_pw:
                        _ss["admin_authorized"] = True
                        st.rerun()
                    elif entered_pw:
                        st.error("Incorrect password.")
                    authorized = _ss.get("admin_authorized", False)

            if authorized:

                # ── Refresh data ─────────────────────────────────────────────
                st.markdown('<div class="refresh-btn">', unsafe_allow_html=True)
                if st.button("↺  Refresh data", use_container_width=True, key="refresh_data_btn"):
                    _ss.pop("fresh_df", None)
                    st.cache_data.clear()
                    st.rerun()
                st.markdown('</div>', unsafe_allow_html=True)

                # ── Current dataset summary ──────────────────────────────────
                if raw_df is not None and not raw_df.empty:
                    st.markdown("**Current dataset**")
                    st.caption(
                        f"Rows: **{len(raw_df):,}**  \n"
                        f"Date range: **{raw_df['complete_date'].min()}** → "
                        f"**{raw_df['complete_date'].max()}**  \n"
                        f"Unique dates: **{raw_df['complete_date'].nunique()}**"
                    )

                    # ── Remove a date range ──────────────────────────────────
                    with st.expander("Remove a date range", expanded=False):
                        st.caption(
                            "Permanently deletes all rows in the chosen window "
                            "from the master dataset.  This cannot be undone."
                        )
                        _dr_min = raw_df["complete_date"].min()
                        _dr_max = raw_df["complete_date"].max()
                        _dc1, _dc2 = st.columns(2)
                        with _dc1:
                            del_start = st.date_input(
                                "From", value=_dr_min,
                                min_value=_dr_min, max_value=_dr_max,
                                key="del_start",
                            )
                        with _dc2:
                            del_end = st.date_input(
                                "To", value=_dr_min,
                                min_value=_dr_min, max_value=_dr_max,
                                key="del_end",
                            )
                        _affected = int(
                            ((raw_df["complete_date"] >= del_start) &
                             (raw_df["complete_date"] <= del_end)).sum()
                        )
                        if del_start > del_end:
                            st.error("'From' date must be on or before 'To' date.")
                        elif _affected:
                            st.warning(f"Will delete **{_affected:,}** rows.")

                        if st.button(
                            "Delete this range", type="primary",
                            use_container_width=True, key="btn_del_range",
                            disabled=(del_start > del_end or _affected == 0),
                        ):
                            _ss["pending_delete_range"] = {
                                "start": del_start, "end": del_end
                            }
                            st.rerun()

                    st.markdown("---")

                # ── Upload new file ──────────────────────────────────────────
                st.markdown("**Step 1 — Select XLSX export**")
                new_file = st.file_uploader(
                    "Upload XLSX", type=["xlsx", "xls"], key="admin_upload",
                    label_visibility="collapsed",
                )
                if new_file is not None:
                    if (
                        "staged_bytes" not in _ss
                        or _ss.get("staged_name") != new_file.name
                    ):
                        _ss["staged_bytes"] = new_file.read()
                        _ss["staged_name"]  = new_file.name
                    st.caption(f"Ready: **{new_file.name}**")

                    st.markdown("**Step 2 — Add to master dataset**")
                    if st.button(
                        "Process & add to master",
                        type="primary", use_container_width=True,
                    ):
                        _ss["pending_upload"] = {
                            "bytes": _ss.pop("staged_bytes"),
                            "name":  _ss.pop("staged_name"),
                        }
                        st.rerun()

                # ── Danger zone ──────────────────────────────────────────────
                st.markdown("---")
                st.markdown("**Danger zone**")
                if st.button(
                    "Reset — delete all data", use_container_width=True,
                ):
                    _ss["pending_reset"] = True
                    st.rerun()

    else:
        # No remote storage configured — fall back to in-session file upload
        st.markdown("**Data source:** Local file upload")
        st.caption(
            "Configure GitHub or Google Drive in Streamlit secrets "
            "to enable persistent storage."
        )
        uploaded_files = st.file_uploader(
            "Upload Excel file(s)", type=["xlsx", "xls"], accept_multiple_files=True,
        )
        if uploaded_files:
            files_bytes = tuple((f.name, f.read()) for f in uploaded_files)
            raw_df, merge_log = _load_from_uploads(files_bytes)

    # ── Date / Month selector ───────────────────────────────────────────────────────────────────────
    st.markdown("---")

    if raw_df is not None and not raw_df.empty:
        filtered_df = filter_for_map(raw_df, map_type)

        if view_mode == "Daily":
            available_dates = sorted(filtered_df["complete_date"].unique())

            if not available_dates:
                st.warning("No data found for this map type.")
                st.stop()

            _min_d = available_dates[0]
            _max_d = available_dates[-1]

            # ── Apply any pending navigation from the previous run ──────────
            # Prev/Next buttons cannot set _ss["date_picker"] directly after
            # the date_input widget has rendered (Streamlit raises
            # StreamlitAPIException for widget-owned keys).  Instead they store
            # the target date in "_pending_date" (a plain session-state key),
            # and we apply it here — before the widget renders — which is safe.
            if "_pending_date" in _ss:
                _pending = _ss.pop("_pending_date")
                if _min_d <= _pending <= _max_d:
                    _ss["date_picker"] = _pending

            # ── Initialise / clamp date_picker (safe: widget not yet rendered) ─
            if (
                "date_picker" not in _ss
                or _ss["date_picker"] < _min_d
                or _ss["date_picker"] > _max_d
            ):
                _ss["date_picker"] = _max_d

            if _ss["date_picker"] not in available_dates:
                _dates_arr = pd.to_datetime(available_dates)
                _idx_near  = (
                    (_dates_arr - pd.Timestamp(_ss["date_picker"])).abs().argmin()
                )
                _ss["date_picker"] = available_dates[_idx_near]

            st.markdown("### Date")
            st.caption(
                f"{len(available_dates)} date(s) with data  ·  "
                f"{_min_d} → {_max_d}"
            )

            # ── Render the date picker widget ───────────────────────────────
            # After this line _ss["date_picker"] is owned by the widget.
            # Do NOT assign to _ss["date_picker"] anywhere below this point —
            # use _ss["_pending_date"] instead.
            picked_date = st.date_input(
                "Select date",
                min_value=_min_d,
                max_value=_max_d,
                label_visibility="collapsed",
                key="date_picker",
            )

            # If the user typed a date with no data, show nearest and queue a
            # correction for the next run via _pending_date (not the widget key)
            if picked_date not in available_dates:
                _dates_arr = pd.to_datetime(available_dates)
                _idx_near  = (
                    (_dates_arr - pd.Timestamp(picked_date)).abs().argmin()
                )
                _nearest = available_dates[_idx_near]
                st.caption(
                    f"No data on {picked_date} for **{map_type}** — "
                    f"showing nearest: **{_nearest}**"
                )
                _ss["_pending_date"] = _nearest   # corrected on next rerun
                picked_date = _nearest

            selected_date = picked_date

            # Compute index — safe linear search on the (small) list
            try:
                _cur_idx = available_dates.index(selected_date)
            except ValueError:
                _cur_idx = len(available_dates) - 1
                selected_date = available_dates[_cur_idx]
                _ss["_pending_date"] = selected_date   # queue fix, not widget key

            _nc1, _nc2 = st.columns(2)
            with _nc1:
                if st.button(
                    "◄ Prev", use_container_width=True,
                    disabled=(_cur_idx == 0),
                ):
                    _ss["_pending_date"] = available_dates[_cur_idx - 1]
                    st.rerun()
            with _nc2:
                if st.button(
                    "Next ►", use_container_width=True,
                    disabled=(_cur_idx == len(available_dates) - 1),
                ):
                    _ss["_pending_date"] = available_dates[_cur_idx + 1]
                    st.rerun()

            st.caption(f"Day {_cur_idx + 1} of {len(available_dates)}")

            # ── Hour range slider ─────────────────────────────────────────────────────────────────────
            st.markdown("---")
            st.markdown("### Hour Range")

            def _fmt_h(h: int) -> str:
                hr12 = 12 if h % 12 == 0 else h % 12
                suf  = "AM" if h < 12 else "PM"
                return f"{hr12}:00 {suf}"

            hour_range = st.slider(
                "Hours", 0, 23, (0, 23), label_visibility="collapsed"
            )
            st.caption(f"{_fmt_h(hour_range[0])} → {_fmt_h(hour_range[1])}")

        else:  # Monthly view
            _all_dates    = sorted(filtered_df["complete_date"].unique())
            _avail_months = sorted({(d.year, d.month) for d in _all_dates})

            if not _avail_months:
                st.warning("No data found for this map type.")
                st.stop()

            _month_labels = [
                f"{_cal.month_name[m]} {y}" for y, m in _avail_months
            ]

            st.markdown("### Month")
            _sel_month_label = st.selectbox(
                "Select month",
                _month_labels,
                index=len(_avail_months) - 1,
                label_visibility="collapsed",
            )
            _sel_idx = _month_labels.index(_sel_month_label)
            selected_year, selected_month = _avail_months[_sel_idx]

        # ── Resource allocation ─────────────────────────────────────────────────────────────────────────
        st.markdown("---")
        with st.expander("Resource Allocation", expanded=False):
            st.markdown(
                "Reassign instruments between maps. "
                "Each resource should appear in only one map."
            )
            new_assignments = {}
            for mt in MAP_TYPES:
                new_assignments[mt] = st.multiselect(
                    mt, options=ALL_RESOURCES,
                    default=_ss.resource_assignments.get(mt, []),
                    key=f"res_{mt}",
                )
            _flat  = [r for rs in new_assignments.values() for r in rs]
            _dupes = sorted({r for r in _flat if _flat.count(r) > 1})
            if _dupes:
                st.warning(f"Duplicate assignments: {', '.join(_dupes)}")

            _ra, _rb = st.columns(2)
            with _ra:
                if st.button("Apply", use_container_width=True, type="primary"):
                    _ss.resource_assignments = new_assignments
                    st.cache_data.clear()
                    st.rerun()
            with _rb:
                if st.button("Reset defaults", use_container_width=True):
                    _ss.resource_assignments = deepcopy(DEFAULT_RESOURCES)
                    st.cache_data.clear()
                    st.rerun()


# ═════════════════════════════════════════════════════════════════════════════
# PENDING UPLOAD PROCESSING
# Runs in the main panel (not the sidebar) so st.status is fully visible.
# ═════════════════════════════════════════════════════════════════════════════
if _ss.get("pending_upload"):
    upload_info = _ss["pending_upload"]
    _render_header(map_type if "map_type" in dir() else "—", "Processing upload…")

    with st.status(f"Processing  {upload_info['name']}…", expanded=True) as _upload_status:
        try:
            # Step 1 — parse the uploaded file
            st.write(f"**Step 1 / 4** — Parsing `{upload_info['name']}`…")
            new_df = parse_single_file(upload_info["bytes"], filename=upload_info["name"])
            st.write(
                f"Parsed **{len(new_df):,}** rows  "
                f"({new_df['complete_date'].min()} → {new_df['complete_date'].max()})"
            )

            # Step 2 — load existing master (read-only until merge is validated)
            st.write("**Step 2 / 4** — Loading existing master dataset…")
            existing_df = pd.DataFrame()
            try:
                if GITHUB_CONFIGURED:
                    sha_up = _get_github_sha()
                    if sha_up:
                        existing_df = _parquet_bytes_to_df(_read_parquet_from_github())
                        st.write(f"Existing master: **{len(existing_df):,}** rows")
                    else:
                        st.write("No existing master — creating a new one.")
                elif DRIVE_CONFIGURED:
                    folder_id_up = st.secrets["google_drive"]["folder_id"]
                    meta_up      = _get_drive_file_meta(folder_id_up)
                    if meta_up:
                        existing_df = _parquet_bytes_to_df(
                            _download_drive_file(meta_up["id"])
                        )
                        st.write(f"Existing master: **{len(existing_df):,}** rows")
                    else:
                        st.write("No existing master — creating a new one.")
            except Exception as _load_ex:
                st.write(f"Could not load existing data ({_load_ex}) — creating new master.")

            # Step 3 — merge + deduplicate entirely in memory
            st.write("**Step 3 / 4** — Merging and deduplicating…")
            if existing_df.empty:
                merged_df  = new_df
                rows_added = len(new_df)
            else:
                merged_df, _stats = merge_new_into_master(existing_df, new_df)
                rows_added = _stats["rows_added"]
            st.write(
                f"Merged: **{len(existing_df):,}** → **{len(merged_df):,}** rows "
                f"(+{rows_added:,} new)"
            )

            # Step 4 — validate the payload then write to remote storage
            st.write("**Step 4 / 4** — Validating and saving…")
            pq_bytes = df_to_parquet_bytes(merged_df)
            _check   = pd.read_parquet(io.BytesIO(pq_bytes))
            if len(_check) == 0:
                raise ValueError(
                    "Parquet round-trip validation failed (0 rows) — aborting write."
                )
            st.write(f"Payload: **{len(pq_bytes) / 1024 / 1024:.2f} MB** (valid)")

            if GITHUB_CONFIGURED:
                _write_parquet_to_github(pq_bytes)
                st.write("Saved to GitHub.")
            elif DRIVE_CONFIGURED:
                _upload_to_drive(st.secrets["google_drive"]["folder_id"], pq_bytes)
                st.write("Saved to Google Drive.")

            # Store merged result in session so the dashboard renders immediately
            # without waiting for the GitHub CDN to reflect the new commit.
            _ss["fresh_df"] = merged_df
            _ss.pop("pending_upload", None)

            _upload_status.update(
                label=f"Done — added {rows_added:,} rows to master dataset.",
                state="complete",
            )
            st.rerun()

        except Exception as _proc_err:
            _upload_status.update(
                label="Processing failed — existing data is unchanged.", state="error"
            )
            st.error(str(_proc_err))
            _ss.pop("pending_upload", None)

    st.stop()


# ═════════════════════════════════════════════════════════════════════════════
# MAIN PANEL — no data guard
# ═════════════════════════════════════════════════════════════════════════════
if raw_df is None or raw_df.empty:
    st.markdown(f"""
    <div class="keck-header">
      <div>
        <h1>Lab Productivity Dashboard</h1>
        <p class="subtitle">Laboratory Analytics</p>
      </div>
      <span class="keck-badge">Laboratory Analytics</span>
    </div>
    """, unsafe_allow_html=True)
    st.info(
        "Welcome!  Upload a file or configure GitHub / Google Drive in your "
        "Streamlit secrets to start viewing lab productivity heatmaps."
    )
    st.stop()


# ═════════════════════════════════════════════════════════════════════════════
# MAIN PANEL — heatmap
# ═════════════════════════════════════════════════════════════════════════════
# MAIN PANEL — heatmap (branched by view mode)
# ═════════════════════════════════════════════════════════════════════════════

if view_mode == "Daily":
    # ── Build pivot ────────────────────────────────────────────────────────────────────────────
    pivot, df_date_hour, df_date, hours = build_pivot(filtered_df, selected_date, hour_range)

    date_str = pd.Timestamp(selected_date).strftime("%B %d, %Y")
    _render_header(map_type, date_str)

    if pivot is None:
        st.warning(
            f"No data found for **{map_type}** on **{date_str}** "
            f"within the selected hour range.  Try widening the hour slider."
        )
        st.stop()

    # ── Metrics row ────────────────────────────────────────────────────────────────────────────
    _hour_cols  = [c for c in pivot.columns if c != "Total"]
    total_vol   = int(pivot["Total"].sum())
    top_proc    = pivot["Total"].idxmax()
    peak_hour   = pivot[_hour_cols].sum().idxmax()
    num_procs   = len(pivot)
    avg_per_hr  = round(total_vol / max(len(_hour_cols), 1), 1)

    _m1, _m2, _m3, _m4, _m5 = st.columns(5)
    with _m1:
        st.markdown(_metric_card("Total Volume", f"{total_vol:,}", accent=True),
                    unsafe_allow_html=True)
    with _m2:
        _tp_disp = top_proc[:28] + "…" if len(top_proc) > 28 else top_proc
        st.markdown(_metric_card("Top Procedure", _tp_disp, sub=f"{int(pivot.loc[top_proc, 'Total']):,} total"),
                    unsafe_allow_html=True)
    with _m3:
        st.markdown(_metric_card("Peak Hour", peak_hour,
                                 sub=f"{int(pivot[_hour_cols].sum()[peak_hour])} completions"),
                    unsafe_allow_html=True)
    with _m4:
        st.markdown(_metric_card("Procedures", str(num_procs), sub="shown (top 30)"),
                    unsafe_allow_html=True)
    with _m5:
        st.markdown(_metric_card("Avg / Hour", str(avg_per_hr),
                                 sub=f"across {len(_hour_cols)} hours"),
                    unsafe_allow_html=True)

    st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

    # ── Heatmap table ─────────────────────────────────────────────────────────────────────────────
    st.markdown(
        '<div class="section-heading">Completed Volume by Procedure &amp; Hour</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="heatmap-legend">'
        f'Colour scale: &nbsp;<strong style="color:#f5e642;">■</strong> low &nbsp;→&nbsp; '
        f'<strong style="color:#3b0f70;">■</strong> high (≥ {VMAX[map_type]} / hour). &nbsp;'
        f'<strong>Total</strong> column = full-day sum per procedure.'
        f'</div>',
        unsafe_allow_html=True,
    )

    _table_h = min(80 + 35 * len(pivot), 900)
    st.dataframe(style_pivot(pivot, VMAX[map_type]), use_container_width=True, height=_table_h)

    # ── PNG download (lazy — rendered only on explicit request) ─────────────────────────────────────────
    _file_prefix = map_type.replace(" ", "_")
    _date_tag    = pd.Timestamp(selected_date).strftime("%Y-%m-%d")

    if st.button("Generate PNG for download"):
        _ss["show_png"] = True

    if _ss.get("show_png"):
        with st.spinner("Rendering PNG…"):
            _png_bytes = build_png(df_date_hour, map_type, selected_date, hours)
        st.download_button(
            label="⬇  Download PNG",
            data=_png_bytes,
            file_name=f"{_file_prefix}_Top30_{_date_tag}.png",
            mime="image/png",
        )

    st.markdown("---")

    # ── Hourly bar chart ──────────────────────────────────────────────────────────────────────────
    with st.expander("Hourly volume bar chart", expanded=False):
        _hourly = pivot[_hour_cols].sum().reset_index()
        _hourly.columns = ["Hour", "Total Volume"]
        st.bar_chart(_hourly.set_index("Hour"), height=220)

    # ── Cell drill-down ────────────────────────────────────────────────────────────────────────────
    with st.expander("Drill into a cell — individual completion events", expanded=False):
        st.markdown(
            "Select a **procedure** and **hour** to inspect every individual "
            "completion event recorded in that cell."
        )
        _dd1, _dd2 = st.columns(2)
        with _dd1:
            sel_proc       = st.selectbox("Procedure", pivot.index.tolist(), key="drill_proc")
        with _dd2:
            sel_hour_label = st.selectbox("Hour", _hour_cols, key="drill_hour")

        sel_hour_int = LABEL_TO_HOUR[sel_hour_label]
        detail       = df_date[
            (df_date["Order Procedure"] == sel_proc) &
            (df_date["hour"] == sel_hour_int)
        ].copy().sort_values("Date/Time - Complete")

        _show_cols = {k: v for k, v in {
            "Date/Time - Complete":        "Completed At",
            "Performing Service Resource": "Resource",
            "Complete Volume":             "Volume",
        }.items() if k in detail.columns}

        detail_display = (
            detail[list(_show_cols.keys())]
            .rename(columns=_show_cols)
            .reset_index(drop=True)
        )
        if "Completed At" in detail_display.columns:
            detail_display["Completed At"] = (
                pd.to_datetime(detail_display["Completed At"])
                .dt.strftime("%Y-%m-%d  %H:%M:%S")
            )

        if detail_display.empty:
            st.info(
                f"No completions for **{sel_proc}** during **{sel_hour_label}** on {date_str}."
            )
        else:
            _cell_vol = (
                int(detail_display["Volume"].sum())
                if "Volume" in detail_display.columns else len(detail_display)
            )
            st.markdown(
                f"**{len(detail_display)} event(s)** &nbsp;·&nbsp; *{sel_proc}* &nbsp;·&nbsp; "
                f"**{sel_hour_label}** &nbsp;·&nbsp; Total volume: **{_cell_vol}**"
            )
            st.dataframe(
                detail_display, use_container_width=True,
                height=min(80 + 35 * len(detail_display), 500),
            )

else:
    # ── Monthly view ──────────────────────────────────────────────────────────────────────────────────
    month_name_str = f"{_cal.month_name[selected_month]} {selected_year}"
    _render_header(map_type, month_name_str)

    monthly_pivot, n_days, month_raw_df = build_monthly_pivot(
        filtered_df, selected_year, selected_month
    )

    if monthly_pivot is None:
        st.warning(f"No data found for **{map_type}** in **{month_name_str}**.")
        st.stop()

    # ── Monthly metrics row ───────────────────────────────────────────────────────────────────
    _m_hour_cols  = [c for c in monthly_pivot.columns if c != "Total"]
    _m_total_vol  = int(round(monthly_pivot["Total"].sum() * n_days))
    _m_top_proc   = monthly_pivot["Total"].idxmax()
    _m_peak_col   = monthly_pivot[_m_hour_cols].sum().idxmax()
    _m_peak_disp  = _m_peak_col.replace("AM", " AM").replace("PM", " PM")
    _m_n_procs    = len(monthly_pivot)
    _m_avg_per_day = round(_m_total_vol / max(n_days, 1))

    _mm1, _mm2, _mm3, _mm4, _mm5 = st.columns(5)
    with _mm1:
        st.markdown(_metric_card("Total Volume", f"{_m_total_vol:,}", accent=True),
                    unsafe_allow_html=True)
    with _mm2:
        _mtp_disp = _m_top_proc[:28] + "…" if len(_m_top_proc) > 28 else _m_top_proc
        st.markdown(_metric_card("Top Procedure", _mtp_disp,
                                 sub=f"highest volume in {month_name_str}"),
                    unsafe_allow_html=True)
    with _mm3:
        st.markdown(_metric_card("Peak Hour", _m_peak_disp,
                                 sub="highest avg volume"), unsafe_allow_html=True)
    with _mm4:
        st.markdown(_metric_card("Procedures Shown", str(_m_n_procs),
                                 sub="top 30 by month volume"), unsafe_allow_html=True)
    with _mm5:
        st.markdown(_metric_card("Avg / Day", f"{_m_avg_per_day:,}",
                                 sub=f"over {n_days} days"), unsafe_allow_html=True)

    st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

    # ── Monthly heatmap title ────────────────────────────────────────────────────────────────────
    st.markdown(
        f'<div class="section-heading">'
        f'{map_type} — Monthly Average | {month_name_str} | N = {n_days} days'
        f'</div>',
        unsafe_allow_html=True,
    )
    st.markdown(
        f'<div class="heatmap-legend">'
        f'Values = avg completed volume per day in hour. '
        f'Colour scale: &nbsp;<strong style="color:#f5e642;">■</strong> low &nbsp;→&nbsp; '
        f'<strong style="color:#3b0f70;">■</strong> high (≥ {VMAX[map_type]}). &nbsp;'
        f'<strong>Total</strong> column = avg daily total per procedure.'
        f'</div>',
        unsafe_allow_html=True,
    )

    _m_table_h = min(80 + 35 * len(monthly_pivot), 900)
    st.dataframe(
        style_monthly_pivot(monthly_pivot, VMAX[map_type]),
        use_container_width=True, height=_m_table_h,
    )

# ── File merge log (only shown when using direct file upload) ─────────────────────────────
if merge_log:
    with st.expander("Data file overlap details", expanded=False):
        st.markdown(
            "Shows how uploaded files were combined.  "
            "Overlapping time windows are trimmed so rows in the overlap "
            "are taken from the later (more complete) file only."
        )
        st.dataframe(pd.DataFrame(merge_log), use_container_width=True)
