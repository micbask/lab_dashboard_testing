"""
ui_components.py — CSS injection, header, metric cards, pivot styling, PNG export.

Contains all visual/presentation logic extracted from the monolithic app.py.
Changes from original:
  - Forecast heatmaps use Oranges colormap (issue #8)
  - Button text-shadow for better visibility (issue #9)
  - Login overlay CSS transition for instant hide (issue #10)
  - Nav uses st.button + st.session_state instead of <a href> / st.query_params
    to prevent page reload / session loss and query_params race conditions (#11)
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
/* ═══════════════════════════════════════════════════════
   BASE — background & font
   ═══════════════════════════════════════════════════════ */
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

/* ═══════════════════════════════════════════════════════
   SIDEBAR — dark background, light text, locked to 320 px
   ═══════════════════════════════════════════════════════
   Width is fixed so the user can't drag-resize the sidebar.
   320 px gives enough room for the section labels and the
   date input on a single line without wrapping. The resize
   handle (split into [data-testid=stSidebarResizer] in some
   Streamlit versions and a bare role="separator" in others)
   is hidden via both selectors so the right edge has no
   visible drag affordance. */
[data-testid="stSidebar"],
section[data-testid="stSidebar"] {
    background-color: #1a1a1a !important;
    border-right: 1px solid #2e2e2e !important;
    width: 320px !important;
    min-width: 320px !important;
    max-width: 320px !important;
}
section[data-testid="stSidebar"] > div:first-child {
    width: 320px !important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarResizer"],
section[data-testid="stSidebar"] div[role="separator"] {
    display: none !important;
}
/* SIDEBAR SECTION LABELS — every section label is rendered as a
   markdown h3 ("### Date" etc.). Streamlit's default theming gives
   these a bold weight + saturated gold colour via emotion-cache
   classes with high specificity, so override with `html body
   section[data-testid="stSidebar"]` plus !important to beat them.
   Includes h1/h2/h4/h5 + [data-testid="stHeading"] as fallbacks in
   case the markdown render path changes in future Streamlit versions. */
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h1,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h2,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h3,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h4,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] h5,
html body section[data-testid="stSidebar"] [data-testid="stHeading"] h1,
html body section[data-testid="stSidebar"] [data-testid="stHeading"] h2,
html body section[data-testid="stSidebar"] [data-testid="stHeading"] h3,
html body section[data-testid="stSidebar"] [data-testid="stHeading"] h4,
html body section[data-testid="stSidebar"] [data-testid="stHeading"] h5,
html body section[data-testid="stSidebar"] h3 {
    color: rgba(241, 171, 31, 0.75) !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    text-transform: uppercase !important;
    letter-spacing: 0.08em !important;
    margin-bottom: 6px !important;
    margin-top: 16px !important;
    line-height: 1.4 !important;
    padding: 0 !important;
}
/* The first sidebar section label has no top margin so it sits flush
   under the sidebar's own top padding rather than picking up a double
   gap. */
html body section[data-testid="stSidebar"] [data-testid="stVerticalBlock"]
    > [data-testid="stElementContainer"]:first-child
    [data-testid="stMarkdownContainer"] h1,
html body section[data-testid="stSidebar"] [data-testid="stVerticalBlock"]
    > [data-testid="stElementContainer"]:first-child
    [data-testid="stMarkdownContainer"] h2,
html body section[data-testid="stSidebar"] [data-testid="stVerticalBlock"]
    > [data-testid="stElementContainer"]:first-child
    [data-testid="stMarkdownContainer"] h3 {
    margin-top: 0 !important;
}
/* Small muted caption used for date ranges + hour-range readouts under
   their respective inputs. 10 px / 40 % white, no extra spacing.
   Higher specificity needed because Streamlit's
   "[data-testid=stMarkdownContainer] p" rule otherwise wins and re-
   applies 0.82rem / weight 500 / #e8e8e8 to the inner text. */
html body section[data-testid="stSidebar"] .sidebar-meta-caption,
html body section[data-testid="stSidebar"] .sidebar-meta-caption p,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] .sidebar-meta-caption,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] .sidebar-meta-caption p {
    font-size: 10px !important;
    color: rgba(255, 255, 255, 0.4) !important;
    margin-top: 6px !important;
    margin-bottom: 0 !important;
    line-height: 1.4 !important;
    font-weight: 400 !important;
    letter-spacing: 0 !important;
    text-transform: none !important;
    font-family: 'Inter', system-ui, sans-serif !important;
}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
[data-testid="stSidebar"] label {
    color: #e8e8e8 !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
}
/* Manual section labels. Rendered via st.markdown('<div
   class="sidebar-section-label">…</div>') from the analytics +
   pre-analytics sidebars. Manual labels sidestep Streamlit's
   high-specificity heading CSS so the subdued look stays consistent. */
html body section[data-testid="stSidebar"] .sidebar-section-label,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] .sidebar-section-label {
    font-family: 'Inter', system-ui, sans-serif !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    color: rgba(241, 171, 31, 0.65) !important;
    margin-top: 20px !important;
    margin-bottom: 8px !important;
    line-height: 1.4 !important;
    padding: 0 !important;
}
/* First label has no top margin so it sits flush under the sidebar
   top padding. Each label is rendered in its own stElementContainer,
   so :first-of-type on the class itself wouldn't disambiguate — key
   off the first stElementContainer of the sidebar vertical block. */
html body section[data-testid="stSidebar"] [data-testid="stVerticalBlock"]
    > [data-testid="stElementContainer"]:first-child
    .sidebar-section-label {
    margin-top: 0 !important;
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

/* ═══════════════════════════════════════════════════════
   BUTTONS — USC Maroon with improved visibility (issue #9)
   ═══════════════════════════════════════════════════════ */
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

/* ═══════════════════════════════════════════════════════
   SELECTBOXES / DROPDOWNS
   ═══════════════════════════════════════════════════════ */
/* Default selectbox trigger (used in the main panel: TAT proc filter
   pop-up, drill-down procedure/hour pickers, admin date-range editor
   inside the Data Management expander). Maroon fill is preserved
   here because these triggers sit on the WHITE main-area background;
   sidebar selectboxes override this to a subtle dark fill below. */
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

/* Sidebar selectbox trigger — subtle dark fill on dark sidebar. Used
   by the Monthly month picker on both analytics + pre-analytics. */
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] > div > div,
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [role="combobox"],
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child {
    background: rgba(255, 255, 255, 0.05) !important;
    background-color: rgba(255, 255, 255, 0.05) !important;
    color: #ffffff !important;
    border: 1px solid rgba(255, 255, 255, 0.15) !important;
    border-radius: 6px !important;
    padding: 8px 12px !important;
}
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child:focus-within,
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child[aria-expanded="true"] {
    background: rgba(255, 255, 255, 0.05) !important;
    background-color: rgba(255, 255, 255, 0.05) !important;
    border-color: #F1AB1F !important;
}
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] svg {
    fill: rgba(255, 255, 255, 0.7) !important;
    color: rgba(255, 255, 255, 0.7) !important;
}

/* NOTE: previously we styled [data-baseweb="popover"] / [role="listbox"]
   / [role="option"] to dark-theme the selectbox dropdown panel. That
   global rule also matched the month / year selectors inside the
   st.date_input calendar popup, which painted the calendar header
   bar dark + the header text dark = an invisible "May 2026" label.
   The rule has been removed; selectbox dropdowns and the calendar
   popup now use Streamlit's default light styling.  The sidebar
   selectbox TRIGGER above keeps its dark fill — only the open-state
   dropdown panel reverts to default. */

/* ═══════════════════════════════════════════════════════
   DATE INPUT
   ═══════════════════════════════════════════════════════ */
/* Default styling for date inputs OUTSIDE the sidebar (admin date-
   range editor lives inside its own expander where this still
   applies). */
[data-testid="stDateInput"] input {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border: 1px solid #57121f !important;
    border-radius: 5px !important;
}
[data-testid="stDateInput"] > div > div > div > svg {
    fill: #ffffff !important;
    color: #ffffff !important;
}
/* Sidebar date inputs get a much subtler treatment — no maroon fill,
   subtle outline, gold focus ring. The value text MUST be readable
   white (#ffffff) — the global `[data-testid=stTextInput] input`
   rule and baseweb's own colour rules otherwise dim the digits. */
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] input,
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"],
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"] > div,
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"] input {
    background: rgba(255, 255, 255, 0.05) !important;
    background-color: rgba(255, 255, 255, 0.05) !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    caret-color: #ffffff !important;
    border: 1px solid rgba(255, 255, 255, 0.15) !important;
    border-radius: 6px !important;
    padding: 8px 12px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    font-family: 'Inter', system-ui, sans-serif !important;
}
/* Inner baseweb wrappers (which carry their own bg / border) need to
   be transparent so the input's own bg+border above is what shows. */
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"],
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"] > div {
    background: transparent !important;
    background-color: transparent !important;
    border: none !important;
    padding: 0 !important;
}
/* Calendar icon inside the input — render light so it's visible on the
   dark sidebar fill. */
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] svg {
    fill: rgba(255, 255, 255, 0.7) !important;
    color: rgba(255, 255, 255, 0.7) !important;
}
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] input:focus {
    border-color: #F1AB1F !important;
    outline: none !important;
    box-shadow: 0 0 0 1px rgba(241, 171, 31, 0.35) !important;
}

/* ═══════════════════════════════════════════════════════
   DATE PICKER POPUP
   ═══════════════════════════════════════════════════════
   No custom CSS — the previous dark-theme override broke empty
   placeholder cells (rendered as solid white blocks), hover state
   (white circle clashing with the maroon selected day), and the
   month / year select. Streamlit's default light calendar is clean
   and professional; a light popup against the dark trigger input is
   an acceptable trade-off vs. a half-broken dark theme.
   ═══════════════════════════════════════════════════════ */

/* ═══════════════════════════════════════════════════════
   SIDEBAR SLIDER  — hide the redundant 0/23 min/max tick-bar
   labels above the track so the only readout is the
   .sidebar-meta-caption beneath ("12:00 AM → 11:00 PM").
   The selected-value bubble on the thumb is kept (it appears
   only on hover/drag); only the fixed min/max endpoint labels
   are hidden.
   ═══════════════════════════════════════════════════════ */
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stSliderTickBar"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stSliderTickBarMin"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stSliderTickBarMax"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stTickBar"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stTickBarMin"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stTickBarMax"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] div[class*="StyledTickBar"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] div[class*="StyledTickBarItem"] {
    display: none !important;
    visibility: hidden !important;
    height: 0 !important;
    margin: 0 !important;
    padding: 0 !important;
}

/* ═══════════════════════════════════════════════════════
   PREV / NEXT DATE NAV BUTTONS
   ═══════════════════════════════════════════════════════
   Arrow-only icon buttons (← and →) that fill their
   parent st.column. Each column is half-sidebar-width so
   on the locked 320 px sidebar the buttons are roughly
   ~140 px wide and visually balanced; the column gap
   ("medium" = 16 px) is the spacing between them.
   Transparent fill + subtle white outline; functionality
   stays identical. */
html body [data-testid="stSidebar"] .st-key-nav_prev_date button,
html body [data-testid="stSidebar"] .st-key-nav_next_date button,
html body [data-testid="stSidebar"] .st-key-nav_prev_date .stButton > button,
html body [data-testid="stSidebar"] .st-key-nav_next_date .stButton > button {
    background: transparent !important;
    background-color: transparent !important;
    color: rgba(255, 255, 255, 0.7) !important;
    border: 1px solid rgba(255, 255, 255, 0.12) !important;
    font-size: 16px !important;
    font-weight: 500 !important;
    padding: 6px 0 !important;
    border-radius: 6px !important;
    text-align: center !important;
    line-height: 1 !important;
    box-shadow: none !important;
    text-shadow: none !important;
    min-height: 0 !important;
}
/* The inner <p> Streamlit wraps the button label in inherits font-
   size from its own emotion class — re-apply 16px there too so the
   arrow glyph is the right size. */
html body [data-testid="stSidebar"] .st-key-nav_prev_date button p,
html body [data-testid="stSidebar"] .st-key-nav_next_date button p,
html body [data-testid="stSidebar"] .st-key-nav_prev_date button div,
html body [data-testid="stSidebar"] .st-key-nav_next_date button div {
    font-size: 16px !important;
    font-weight: 500 !important;
    color: inherit !important;
    line-height: 1 !important;
    margin: 0 !important;
    padding: 0 !important;
}
html body [data-testid="stSidebar"] .st-key-nav_prev_date button:hover,
html body [data-testid="stSidebar"] .st-key-nav_next_date button:hover {
    background: rgba(255, 255, 255, 0.05) !important;
    background-color: rgba(255, 255, 255, 0.05) !important;
    color: #ffffff !important;
    border-color: rgba(255, 255, 255, 0.2) !important;
}
html body [data-testid="stSidebar"] .st-key-nav_prev_date button:disabled,
html body [data-testid="stSidebar"] .st-key-nav_next_date button:disabled {
    background: transparent !important;
    background-color: transparent !important;
    border-color: rgba(255, 255, 255, 0.06) !important;
    color: rgba(255, 255, 255, 0.25) !important;
    opacity: 0.6 !important;
}

/* ═══════════════════════════════════════════════════════
   RADIO BUTTONS
   ═══════════════════════════════════════════════════════ */
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

/* ═══════════════════════════════════════════════════════
   SIDEBAR RADIO BUTTONS → TEXT TAB STYLE
   ═══════════════════════════════════════════════════════
   Replace the filled-circle radio visual with a clean
   underlined-text-tab look: unselected options render in
   muted white, the selected option gets gold text + a 2 px
   gold underline. Underlying widget (st.radio) is unchanged
   so all selection logic / session state keeps working.
   :has(input:checked) handles the selected-state detection
   without any JS — supported in modern browsers (Chrome
   105+, Firefox 121+, Safari 15.4+), which covers the
   Streamlit Cloud target.
   Only applies inside the sidebar; main-area radios (if any
   ever appear) keep their default styling. */

/* Hide the baseweb radio circle indicator. Streamlit wraps
   each option's <input> in a div that holds the visual dot
   — that's the first <div> child of the <label>. The label's
   text is the second <div> child, which stays visible. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label > div:first-child {
    display: none !important;
}

/* Horizontal tab row — st.radio(horizontal=True) already does
   this, but we force display:flex / gap explicitly so vertical
   radio groups also pick up the tab look, and the spec'd 24 px
   gap is exact regardless of Streamlit's emotion-cache defaults. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] {
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: wrap !important;
    gap: 24px !important;
    align-items: flex-end !important;
}

/* Each <label> becomes the tab. Padding gives the underline
   somewhere to sit below the text without crowding it. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label {
    color: rgba(255, 255, 255, 0.5) !important;
    font-weight: 500 !important;
    font-size: 14px !important;
    padding: 4px 0 8px 0 !important;
    border-bottom: 2px solid transparent !important;
    cursor: pointer !important;
    margin: 0 !important;
    background: transparent !important;
    background-color: transparent !important;
    transition: color 0.15s ease, border-color 0.15s ease !important;
}

/* Inner text wrapper — Streamlit nests the option label in a
   <div> (sometimes containing a <p>). Force both to inherit
   the label's colour/weight so the tab gold/white flips on the
   text itself, not just the bottom border. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label > div,
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label p,
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label span {
    color: inherit !important;
    font-weight: inherit !important;
    font-size: inherit !important;
    margin: 0 !important;
    padding: 0 !important;
    background: transparent !important;
    background-color: transparent !important;
}

/* Hover (unselected only — the :not() guard keeps the selected
   tab's gold from dimming on hover). */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label:hover:not(:has(input:checked)) {
    color: rgba(255, 255, 255, 0.8) !important;
    border-bottom-color: transparent !important;
}

/* Selected tab — gold text, gold 2 px underline. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label:has(input:checked) {
    color: #F1AB1F !important;
    border-bottom-color: #F1AB1F !important;
    font-weight: 500 !important;
}

/* ═══════════════════════════════════════════════════════
   TEXT INPUTS
   ═══════════════════════════════════════════════════════ */
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

/* ═══════════════════════════════════════════════════════
   METRIC CARDS
   ═══════════════════════════════════════════════════════ */
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

/* ═══════════════════════════════════════════════════════
   EXPANDERS
   ═══════════════════════════════════════════════════════ */
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

/* ═══════════════════════════════════════════════════════
   DIVIDERS
   ═══════════════════════════════════════════════════════ */
hr {
    border-color: #e2e8f0 !important;
    margin: 1.1rem 0 !important;
}

/* ═══════════════════════════════════════════════════════
   CUSTOM COMPONENT CLASSES
   ═══════════════════════════════════════════════════════ */
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

/* ═══════════════════════════════════════════════════════
   LOGIN OVERLAY — instant hide after auth (issue #10)
   ═══════════════════════════════════════════════════════ */
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
    """Render the Keck Medicine banner.

    Layout: title block on the left (h1 "Laboratory Productivity
    Dashboard" + gold subtitle, matching main_test_2 typography);
    a small st.button-based segmented-control tab bar pinned to the
    top-right with the date below it. Banner is slimmer than the
    earlier version (tighter vertical padding + smaller buttons).

    Nav clicks write st.query_params and call st.rerun(), so the URL
    and active dashboard stay in sync without any anchor-tag reload.
    """
    _active = st.session_state.get(
        "_nav_dashboard",
        st.query_params.get("dashboard", "analytics"),
    )
    _active_key = "nav_analytics" if _active == "analytics" else "nav_pre_analytics"

    # Build the banner-scoped selector once for legibility.
    _banner_sel = (
        'div[data-testid="stHorizontalBlock"]'
        ':has(.keck-header-title):has(.st-key-nav_analytics)'
    )
    _pill_sel = (
        'div[data-testid="stHorizontalBlock"]'
        ':has(.st-key-nav_analytics):not(:has(.keck-header-title))'
    )

    st.markdown(f"""
    <style>
      /* ── Outer maroon banner ── positioning context for the date.
            Vertical padding bumped from 0.85rem → 1.2rem to add a
            visible gap between the nav pill (pinned to the top via
            normal flow) and the date (pinned 0.85rem from the bottom
            via absolute positioning). */
      {_banner_sel} {{
          position: relative !important;
          background: linear-gradient(135deg, #6F1828 0%, #521322 100%);
          padding: 1.2rem 1.8rem !important;
          border-radius: 10px !important;
          margin-bottom: 1.4rem !important;
          box-shadow: 0 2px 8px rgba(111,24,40,0.25);
          align-items: stretch !important;
      }}
      /* Force every wrapper between the banner and the date back to
         static positioning so the date's `position: absolute` actually
         anchors to the banner (Streamlit puts position: relative on a
         number of intermediate stColumn / stVerticalBlock wrappers). */
      {_banner_sel} [data-testid="stColumn"],
      {_banner_sel} [data-testid="stVerticalBlock"],
      {_banner_sel} [data-testid="stElementContainer"],
      {_banner_sel} [data-testid="stMarkdownContainer"],
      {_banner_sel} [data-testid="element-container"] {{
          position: static !important;
      }}

      /* ── Title typography ── high-specificity to beat Streamlit's
            default <h1> / <p> margins which otherwise space the
            subtitle far below the title. */
      html body {_banner_sel} .keck-header-title,
      html body {_banner_sel} .keck-header-title * {{
          line-height: 1.2 !important;
      }}
      html body {_banner_sel} .keck-header-title h1 {{
          color: #ffffff !important;
          font-size: 1.5rem !important;
          font-weight: 700 !important;
          letter-spacing: 0.2px !important;
          margin: 0 !important;
          padding: 0 !important;
          line-height: 1.2 !important;
      }}
      html body {_banner_sel} .keck-header-title p,
      html body {_banner_sel} .keck-header-title p.subtitle {{
          color: #EDC153 !important;
          font-size: 0.87rem !important;
          font-weight: 500 !important;
          margin: 0.2rem 0 0 0 !important;
          padding: 0 !important;
          opacity: 0.95 !important;
          line-height: 1.2 !important;
      }}

      /* ── Inner pill container ── pinned right inside its column. */
      {_pill_sel} {{
          display: flex !important;
          width: fit-content !important;
          max-width: fit-content !important;
          margin-left: auto !important;
          background: rgba(0,0,0,0.25) !important;
          border-radius: 7px !important;
          padding: 3px !important;
          gap: 2px !important;
      }}
      {_pill_sel} > div {{
          flex: 0 0 auto !important;
          width: auto !important;
          min-width: 0 !important;
      }}

      /* ── Nav buttons ── target both the <button> and the inner <p>
            (Streamlit puts the label inside a <p> in a markdown
            container, which has its own font-size / color). */
      html body {_pill_sel} button,
      html body {_pill_sel} [data-testid="stBaseButton-secondary"],
      html body {_pill_sel} .stButton > button {{
          background: transparent !important;
          background-color: transparent !important;
          color: rgba(255,255,255,0.6) !important;
          border: none !important;
          border-radius: 5px !important;
          padding: 4px 14px !important;
          box-shadow: none !important;
          min-width: 90px !important;
          min-height: 0 !important;
          height: auto !important;
          line-height: 1 !important;
          text-shadow: none !important;
          outline: none !important;
      }}
      html body {_pill_sel} button p,
      html body {_pill_sel} button div {{
          color: rgba(255,255,255,0.6) !important;
          font-size: 11px !important;
          font-weight: 700 !important;
          letter-spacing: 0.06em !important;
          margin: 0 !important;
          padding: 0 !important;
          line-height: 1.4 !important;
      }}

      /* Hover on the whole pill, but only show effect on inactive. */
      html body {_pill_sel} button:hover {{
          background: rgba(255,255,255,0.10) !important;
          background-color: rgba(255,255,255,0.10) !important;
          border: none !important;
      }}
      html body {_pill_sel} button:hover p,
      html body {_pill_sel} button:hover div {{
          color: rgba(255,255,255,0.85) !important;
      }}

      /* Kill focus ring everywhere. */
      html body {_pill_sel} button:focus,
      html body {_pill_sel} button:focus-visible {{
          outline: none !important;
          box-shadow: none !important;
      }}

      /* ── ACTIVE BUTTON ── gold pill, white bold text. Must come AFTER
            the hover rule so the hover doesn't dim the active gold. */
      html body {_pill_sel} .st-key-{_active_key} button,
      html body {_pill_sel} .st-key-{_active_key} button:hover,
      html body {_pill_sel} .st-key-{_active_key} button:focus,
      html body .st-key-{_active_key} button,
      html body .st-key-{_active_key} button:hover,
      html body .st-key-{_active_key} button:focus {{
          background: #F1AB1F !important;
          background-color: #F1AB1F !important;
          color: #ffffff !important;
          border: none !important;
      }}
      html body {_pill_sel} .st-key-{_active_key} button p,
      html body {_pill_sel} .st-key-{_active_key} button div,
      html body {_pill_sel} .st-key-{_active_key} button:hover p,
      html body .st-key-{_active_key} button p,
      html body .st-key-{_active_key} button div,
      html body .st-key-{_active_key} button:hover p {{
          color: #ffffff !important;
          font-weight: 700 !important;
      }}

      /* ── Date ── absolutely positioned at the banner's bottom-right
            corner, on the same horizontal line as the gold subtitle. */
      html body {_banner_sel} .keck-header-date {{
          position: absolute !important;
          right: 1.8rem !important;
          bottom: 0.85rem !important;
          color: rgba(255,255,255,0.85) !important;
          font-size: 0.87rem !important;
          font-weight: 500 !important;
          text-align: right !important;
          margin: 0 !important;
          padding: 0 !important;
          line-height: 1 !important;
          z-index: 5 !important;
      }}
    </style>
    """, unsafe_allow_html=True)

    cols = st.columns([0.72, 0.28], vertical_alignment="top")
    with cols[0]:
        # Prefix the subtitle with the active-dashboard name so the
        # banner reads "Analytics · Keck Core" / "Pre-Analytics · Keck"
        # / etc. — matches the new login-page subtitle pattern.
        _prefix = (
            "Pre-Analytics" if _active == "pre_analytics" else "Analytics"
        )
        st.markdown(
            f'''<div class="keck-header-title">
                  <h1>Laboratory Productivity Dashboard</h1>
                  <p class="subtitle">{_prefix} · {map_type}</p>
                </div>''',
            unsafe_allow_html=True,
        )
    with cols[1]:
        nav_cols = st.columns([1, 1])
        with nav_cols[0]:
            if st.button("ANALYTICS", key="nav_analytics",
                         use_container_width=True):
                st.query_params["dashboard"] = "analytics"
                st.rerun()
        with nav_cols[1]:
            if st.button("PRE-ANALYTICS", key="nav_pre_analytics",
                         use_container_width=True):
                st.query_params["dashboard"] = "pre_analytics"
                st.rerun()
        st.markdown(
            f'<div class="keck-header-date">{date_str}</div>',
            unsafe_allow_html=True,
        )


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
    hours: list,
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
