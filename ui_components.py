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
    /* Explicit horizontal padding so the inner content (KPI cards,
       heatmap, section headings) sits 24 px from block-container's
       outer edge. The header bar + cardinal stripe use their own
       full-bleed calc() margins (see render_header / .app-header-stripe
       below) and do NOT depend on this padding to align — they extend
       past block-container's max-width: 1480 px to reach stMain's
       full content area. */
    padding-left: 24px !important;
    padding-right: 24px !important;
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
    position: relative !important;
}
section[data-testid="stSidebar"] > div:first-child {
    width: 320px !important;
}
section[data-testid="stSidebar"] [data-testid="stSidebarResizer"],
section[data-testid="stSidebar"] div[role="separator"] {
    display: none !important;
    pointer-events: none !important;
}
/* Force the cursor to default everywhere on the sidebar — Streamlit
   sets cursor: col-resize / ew-resize on the right-edge resizer
   element (and on its ::before / ::after pseudo-elements in some
   versions) which still triggers the double-arrow cursor on hover
   even when the resizer itself is display:none. We override on every
   plausible carrier so the right edge feels static. */
section[data-testid="stSidebar"],
section[data-testid="stSidebar"]::before,
section[data-testid="stSidebar"] > div,
section[data-testid="stSidebar"] [data-testid="stSidebarResizer"],
section[data-testid="stSidebar"] [data-testid="stSidebarResizer"]::before,
section[data-testid="stSidebar"] [data-testid="stSidebarResizer"]::after,
section[data-testid="stSidebar"] div[role="separator"] {
    cursor: default !important;
}
/* Invisible right-edge overlay — final defense against the col-resize
   cursor. The previous round of fixes targeted every named resizer
   element we could find (stSidebarResizer, div[role=separator], the
   section itself + its pseudo-elements) but the cursor still leaked
   through, which means in this Streamlit version the cursor is being
   applied to an element we can't enumerate (possibly a sibling outside
   the section, possibly via inline style set by Streamlit's JS, or via
   a pseudo-element on a wrapper we don't know the selector for).
   This overlay sidesteps all that: it's an empty ::after pseudo-element
   anchored to the sidebar's right edge, extending 6 px outside and
   6 px inside the edge, with `cursor: default !important`,
   `pointer-events: auto`, and a high z-index. Any cursor-bearing
   element underneath is covered by this strip; hovering at the right
   edge hits the overlay instead and the cursor stays as the default
   arrow. The strip is fully transparent so it's visually a no-op. */
section[data-testid="stSidebar"]::after {
    content: '' !important;
    display: block !important;
    position: absolute !important;
    top: 0 !important;
    right: -6px !important;
    width: 12px !important;
    height: 100% !important;
    background: transparent !important;
    cursor: default !important;
    pointer-events: auto !important;
    z-index: 99999 !important;
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
   by the Monthly month picker on both analytics + pre-analytics.
   Background is a SOLID dark color (not an `rgba(...)` overlay).
   When the theme's `secondaryBackgroundColor` was dark, the previous
   `rgba(255, 255, 255, 0.05)` overlay rendered as ~#232323 (sidebar
   bg showing through 5 % white). After that theme value was reverted
   to the Streamlit-default light `#f0f2f6` to fix the calendar
   header, baseweb's Select control started using the new LIGHT color
   for its internal trigger background — the transparent overlay
   exposed it, leaving white text invisible on a near-white surface.
   A solid `#262626` keeps the trigger dark independent of any theme
   value, so the white text stays readable regardless of future
   theme changes. */
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] > div > div,
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [role="combobox"],
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child {
    background: #262626 !important;
    background-color: #262626 !important;
    color: #ffffff !important;
    border: 1px solid rgba(255, 255, 255, 0.15) !important;
    border-radius: 6px !important;
    padding: 8px 12px !important;
}
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child:focus-within,
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child[aria-expanded="true"] {
    background: #262626 !important;
    background-color: #262626 !important;
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

/* SIDEBAR SLIDER — recolor active track + thumbs to USC gold
   (#F1AB1F). The inactive portion of the track stays gray
   (baseweb default). Two targets:
     1. role="slider"  — the draggable thumb circles. Always
        targetable by ARIA role across baseweb versions.
     2. > div > div > div > div  — the well-known "active fill"
        depth used in every Streamlit slider color-override
        snippet in the wild. Streamlit nests the active highlight
        bar four divs deep inside [data-testid="stSlider"]. */
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [role="slider"] {
    background: #F1AB1F !important;
    background-color: #F1AB1F !important;
    border-color: #F1AB1F !important;
    box-shadow: 0 0 0 1px #F1AB1F !important;
}
html body section[data-testid="stSidebar"] [data-testid="stSlider"] > div > div > div > div {
    background: #F1AB1F !important;
    background-color: #F1AB1F !important;
}
/* Thumb-value bubble (appears on hover/drag) — recolor its text
   to gold so the floating "12" / "23" label matches the track. */
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stThumbValue"] {
    color: #F1AB1F !important;
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

/* 16 px breathing room between the date-range caption above and
   the Prev/Next button row. The :has() selector targets the row
   that contains either nav button, so the gap appears regardless
   of which dashboard renders the row. */
html body section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(.st-key-nav_prev_date),
html body section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(.st-key-nav_next_date) {
    margin-top: 16px !important;
}
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
   METRIC CARDS (st.metric — used in admin tools)
   ═══════════════════════════════════════════════════════ */
[data-testid="metric-container"] {
    background: #ffffff !important;
    border: 0.5px solid rgba(0, 0, 0, 0.08) !important;
    border-radius: 8px !important;
    padding: 14px !important;
    box-shadow: none !important;
}
[data-testid="metric-container"] label {
    font-size: 11px !important;
    font-weight: 500 !important;
    letter-spacing: 0 !important;
    text-transform: none !important;
    color: rgba(0, 0, 0, 0.55) !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 22px !important;
    font-weight: 500 !important;
    color: #1a1a1a !important;
}

/* ═══════════════════════════════════════════════════════
   KPI CARD ROW (custom .metric-card HTML emitted by
   `metric_card()` in ui_components — used by every dashboard
   to render the 4–5 KPI cards above each heatmap)
   ═══════════════════════════════════════════════════════
   All five cards in a row must be equal-height. Streamlit's
   st.columns layout doesn't equalize child heights by
   default — the :has() block below stretches the column +
   inner stVerticalBlock so the .metric-card inside each
   column fills 100 % of the tallest sibling's height. The
   long-value variant (auto-applied by metric_card when the
   value is a procedure name) shrinks just the .value font
   so it can wrap without making the card taller than its
   short-value siblings. */
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card) {
    align-items: stretch !important;
    gap: 8px !important;
}
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card)
    > [data-testid="stColumn"] {
    display: flex !important;
    flex-direction: column !important;
}
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card)
    > [data-testid="stColumn"] > [data-testid="stVerticalBlock"] {
    flex: 1 !important;
    width: 100% !important;
    height: 100% !important;
    display: flex !important;
    flex-direction: column !important;
}
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card)
    > [data-testid="stColumn"] > [data-testid="stVerticalBlock"]
    > [data-testid="stElementContainer"] {
    flex: 1 !important;
    height: 100% !important;
}
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card)
    > [data-testid="stColumn"] [data-testid="stMarkdownContainer"] {
    flex: 1 !important;
    width: 100% !important;
    height: 100% !important;
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
    border: 0.5px solid rgba(0, 0, 0, 0.08);
    border-radius: 8px;
    padding: 14px;
    margin-bottom: 0;
    /* Equal-height enforcement — `min-height: 110 px` is the floor for
       every card in a row regardless of content. Combined with the
       :has() flex-stretch on the parent stHorizontalBlock (defined
       further up), short-value cards (numbers, "9 AM") match the
       tallest sibling — typically the Top procedure card, whose
       value can wrap to 2 lines via .metric-card-long below. */
    min-height: 110px;
    height: 100%;
    box-sizing: border-box;
    box-shadow: none;
    display: flex;
    flex-direction: column;
}
/* `.accent` legacy class is intentionally a no-op now — all cards
   share the same neutral fill in the new design. */
.metric-card.accent {
    border-top-color: rgba(0, 0, 0, 0.08);
}
.metric-card .label {
    color: rgba(0, 0, 0, 0.55);
    font-size: 11px;
    font-weight: 500;
    text-transform: none;
    letter-spacing: 0;
    margin-bottom: 0;
}
.metric-card .value {
    color: #1a1a1a;
    font-size: 22px;
    font-weight: 500;
    line-height: 1;
    margin-top: 10px;
    word-break: break-word;
}
/* Long values (procedure names) shrink to 14 px so they wrap
   without making the card taller than its short-value peers. */
.metric-card.metric-card-long .value {
    font-size: 14px;
    line-height: 1.2;
}
.metric-card .sub {
    color: rgba(0, 0, 0, 0.45);
    font-size: 11px;
    margin-top: 8px;
}
.section-heading {
    color: #1a1a1a;
    font-size: 16px;
    font-weight: 500;
    /* Gold underline restored — subtle 1 px / 50 % USC gold so it
       reads as a quiet rhythm element rather than a hard rule.
       padding-bottom = 8 px so the line sits 8 px below the heading
       text; margin-bottom = 16 px so the line is 16 px above the
       legend; margin-top = 12 px combines with the metrics-divider's
       12 px margin above to reach the spec'd 24 px gap from the KPI
       cards row. */
    border-bottom: 1px solid rgba(241, 171, 31, 0.5);
    padding: 0 0 8px 0;
    letter-spacing: 0;
    text-transform: none;
    margin: 12px 0 16px 0;
}
.heatmap-legend {
    background: rgba(0, 0, 0, 0.02);
    border: 0.5px solid rgba(0, 0, 0, 0.06);
    border-left: 0.5px solid rgba(0, 0, 0, 0.06);
    border-radius: 6px;
    padding: 8px 12px;
    font-size: 12px;
    color: rgba(0, 0, 0, 0.6);
    margin-top: 0;
    margin-bottom: 12px;  /* 12 px gap to the heatmap below */
}
/* ═══════════════════════════════════════════════════════
   APP HEADER STRIPE — 2 px cardinal band that sits directly
   between the dark header bar and the light content area.
   Uses the same full-bleed-minus-sidebar margin formula as
   the bar above (50% - 50vw + 160 px on each side), so the
   stripe runs edge-to-edge with the bar across the full
   content area regardless of block-container's max-width
   1480 px constraint. The cardinal stripe is the only place
   USC's cardinal appears as a background colour in the new
   design.
   ═══════════════════════════════════════════════════════ */
.app-header-stripe {
    height: 2px !important;
    background: #790A26 !important;
    margin-top: 0 !important;
    margin-bottom: 16px !important;
    margin-left: calc(50% - 50vw + 160px) !important;
    margin-right: calc(50% - 50vw + 160px) !important;
    padding: 0 !important;
    border: none !important;
    position: relative !important;
}
.metrics-divider {
    /* Subtle horizontal line between the KPI card row and the
       section heading below. Combined with the 12 px on each side,
       the total gap from KPI cards to the section heading text is
       ~24 px (matches the spec'd vertical-rhythm value). */
    border: none;
    border-top: 1px solid rgba(0, 0, 0, 0.06);
    margin: 12px 0;
    height: 0;
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

def _is_long_kpi_value(v: str) -> bool:
    """Heuristic: does a KPI value need the smaller-font card style?

    Short values (numbers, "9 AM"/"6 PM" hour labels, percentages,
    short tags) keep the 22 px / weight-500 main-value treatment.
    Long values (procedure names like "Magnesium Plasma/Serum",
    "Comprehensive Metabolic Panel…") get 14 px / line-height 1.2
    so they wrap gracefully without breaking row-height alignment.
    """
    s = (v or "").strip()
    if len(s) <= 6:
        return False
    if " AM" in s.upper() or " PM" in s.upper():
        return False
    if all(c.isdigit() or c in ".,%-+:" for c in s):
        return False
    return True


def metric_card(label: str, value: str, sub: str = "", accent: bool = False) -> str:
    """Return HTML for a KPI metric card.

    Uniform style for every card — `accent` is kept as a no-op
    parameter for backwards compatibility with existing call sites
    that pass `accent=True`; the new design has no per-card accent
    treatment (all cards share the same neutral white fill + thin
    border, no coloured top stripe). Long values are auto-detected
    via `_is_long_kpi_value` and rendered at a smaller font so cards
    in the same row stay equal-height.
    """
    _ = accent  # backwards compat — no-op in the new design
    long_cls = " metric-card-long" if _is_long_kpi_value(str(value)) else ""
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    return (
        f'<div class="metric-card{long_cls}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>'
        f'{sub_html}'
        f'</div>'
    )


def render_header(map_type: str, date_str: str) -> None:
    """Render the thin dark app header bar + cardinal stripe.

    Replaces the prior maroon banner with a quieter design that
    visually extends the sidebar's dark chrome across the top of the
    content area. Layout:

      • Left   : "Laboratory productivity dashboard" (17 px / 500)
                 + subtitle reading "{bench/location} · {date}" (12 px /
                 muted white). Subtitle updates reactively with the
                 sidebar selections passed in via map_type + date_str.
      • Right  : Analytics / Pre-Analytics toggle pill — real
                 st.button widgets so the existing query_params
                 mechanism keeps switching dashboards. CSS scoped via
                 `:has()` styles them as the gold-on-dark pill.

    Followed by a 2 px cardinal stripe (#790A26) — the only background
    use of USC's cardinal in the new design. It anchors the brand at
    the chrome/content boundary.

    Both the bar and the stripe use a full-bleed-minus-sidebar margin
    formula (`50% - 50vw + 160 px` on each side) so they extend past
    block-container's max-width 1480 px to reach stMain's full content
    area (sidebar's right edge → viewport's right edge).
    """
    _active = st.session_state.get(
        "_nav_dashboard",
        st.query_params.get("dashboard", "analytics"),
    )
    _active_key = "nav_analytics" if _active == "analytics" else "nav_pre_analytics"

    # Subtitle: "{bench/location} · {date}".
    #   • Analytics passes the full bench name as map_type — "Keck Core",
    #     "Norris Core", "Norris Specialty". These are proper USC
    #     department names; keep Title Case so "Core"/"Specialty" stay
    #     capitalized in the subtitle.
    #   • Pre-analytics passes the short location ("Keck", "Norris",
    #     "HC3") which is also already correctly cased.
    # In both cases map_type passes through unchanged.
    _bench = map_type
    _subtitle = f"{_bench} · {date_str}"

    # Banner-scoped selectors — `:has()` lets us target the specific
    # horizontal block holding the title + nav buttons without affecting
    # any other stHorizontalBlock on the page.
    _banner_sel = (
        'div[data-testid="stHorizontalBlock"]'
        ':has(.app-header-title):has(.st-key-nav_analytics)'
    )
    _pill_sel = (
        'div[data-testid="stHorizontalBlock"]'
        ':has(.st-key-nav_analytics):not(:has(.app-header-title))'
    )

    st.markdown(f"""
    <style>
      /* ── Header bar — dark band that visually extends the sidebar's
            chrome across the top of the content area. Full-bleed via
            calc() margins so it spans past block-container's max-width
            1480 px to stMain's edges (sidebar right → viewport right). */
      {_banner_sel} {{
          position: relative !important;
          background: #1a1a1a !important;
          padding: 20px 24px !important;
          /* FULL-BLEED MINUS SIDEBAR — extend the bar from the
             sidebar's right edge to the viewport's right edge,
             regardless of block-container's max-width: 1480 px
             constraint.

             The formula `margin-x = 50% - 50vw + 160 px` is the
             standard CSS full-bleed trick adapted to skip the
             sidebar:
               • 50%      = parent (block-container) center
               • 50vw     = viewport center
               • + 160 px = half of the locked 320 px sidebar
             When the parent is centered inside stMain (which itself
             spans 100vw - 320 px), this pulls the bar's box out to
             stMain's edges. On viewports narrower than 1480 px the
             formula collapses to 0 (block-container already fills
             stMain naturally), so the bar matches stMain in both
             cases.

             The previous `box-shadow: -100vw 0 0` trick was a no-op
             — the shadow is the same shape/size as the element with
             no spread, so it just painted a duplicate of the bar
             1920 px off-screen. Replaced here with real width via
             negative margins. */
          margin-top: -1.8rem !important;
          margin-bottom: 0 !important;
          margin-left: calc(50% - 50vw + 160px) !important;
          margin-right: calc(50% - 50vw + 160px) !important;
          align-items: center !important;
          gap: 16px !important;
          border-radius: 0 !important;
          box-sizing: border-box !important;
      }}
      /* Force every wrapper inside the banner back to static positioning
         so the bar's own positioning context governs absolute children. */
      {_banner_sel} [data-testid="stColumn"],
      {_banner_sel} [data-testid="stVerticalBlock"],
      {_banner_sel} [data-testid="stElementContainer"],
      {_banner_sel} [data-testid="stMarkdownContainer"] {{
          position: static !important;
      }}

      /* ── Title + subtitle typography. */
      html body {_banner_sel} .app-header-title {{
          color: #ffffff !important;
          font-size: 17px !important;
          font-weight: 500 !important;
          letter-spacing: -0.01em !important;
          line-height: 1.2 !important;
          margin: 0 !important;
          padding: 0 !important;
      }}
      html body {_banner_sel} .app-header-subtitle {{
          color: rgba(255, 255, 255, 0.6) !important;
          font-size: 12px !important;
          font-weight: 400 !important;
          letter-spacing: 0 !important;
          line-height: 1.3 !important;
          margin: 3px 0 0 0 !important;
          padding: 0 !important;
      }}

      /* ── Toggle pill — wraps the two nav buttons on the right of the
            bar. Subtle white-on-dark fill, rounded 6 px, 2 px inner pad. */
      {_pill_sel} {{
          display: flex !important;
          width: fit-content !important;
          max-width: fit-content !important;
          margin-left: auto !important;
          background: rgba(255, 255, 255, 0.06) !important;
          border-radius: 6px !important;
          padding: 2px !important;
          gap: 0 !important;
      }}
      {_pill_sel} > div {{
          flex: 0 0 auto !important;
          width: auto !important;
          min-width: 0 !important;
      }}

      /* ── Inactive nav button — transparent, muted white text. */
      html body {_pill_sel} button,
      html body {_pill_sel} [data-testid="stBaseButton-secondary"],
      html body {_pill_sel} .stButton > button {{
          background: transparent !important;
          background-color: transparent !important;
          color: rgba(255, 255, 255, 0.55) !important;
          border: none !important;
          border-radius: 4px !important;
          padding: 5px 12px !important;
          font-size: 12px !important;
          font-weight: 500 !important;
          line-height: 1.2 !important;
          min-width: 0 !important;
          min-height: 0 !important;
          height: auto !important;
          box-shadow: none !important;
          text-shadow: none !important;
          outline: none !important;
          transition: color 0.15s ease, background 0.15s ease !important;
      }}
      html body {_pill_sel} button p,
      html body {_pill_sel} button div {{
          color: inherit !important;
          font-size: 12px !important;
          font-weight: 500 !important;
          letter-spacing: 0 !important;
          line-height: 1.2 !important;
          margin: 0 !important;
          padding: 0 !important;
      }}
      html body {_pill_sel} button:hover {{
          color: rgba(255, 255, 255, 0.85) !important;
          background: rgba(255, 255, 255, 0.04) !important;
          background-color: rgba(255, 255, 255, 0.04) !important;
      }}
      html body {_pill_sel} button:focus,
      html body {_pill_sel} button:focus-visible {{
          outline: none !important;
          box-shadow: none !important;
      }}

      /* ── Active nav button — gold pill with dark text. Must come AFTER
            the hover rule above so hover doesn't dim the active gold. */
      html body {_pill_sel} .st-key-{_active_key} button,
      html body {_pill_sel} .st-key-{_active_key} button:hover,
      html body {_pill_sel} .st-key-{_active_key} button:focus,
      html body .st-key-{_active_key} button,
      html body .st-key-{_active_key} button:hover,
      html body .st-key-{_active_key} button:focus {{
          background: #F1AB1F !important;
          background-color: #F1AB1F !important;
          color: #171717 !important;
      }}
      html body {_pill_sel} .st-key-{_active_key} button p,
      html body {_pill_sel} .st-key-{_active_key} button div,
      html body .st-key-{_active_key} button p,
      html body .st-key-{_active_key} button div {{
          color: #171717 !important;
          font-weight: 500 !important;
      }}
    </style>
    """, unsafe_allow_html=True)

    cols = st.columns([0.65, 0.35], vertical_alignment="center")
    with cols[0]:
        st.markdown(
            f'<div class="app-header-title">Laboratory productivity dashboard</div>'
            f'<div class="app-header-subtitle">{_subtitle}</div>',
            unsafe_allow_html=True,
        )
    with cols[1]:
        nav_cols = st.columns([1, 1])
        with nav_cols[0]:
            if st.button("Analytics", key="nav_analytics",
                         use_container_width=True):
                st.query_params["dashboard"] = "analytics"
                st.rerun()
        with nav_cols[1]:
            if st.button("Pre-Analytics", key="nav_pre_analytics",
                         use_container_width=True):
                st.query_params["dashboard"] = "pre_analytics"
                st.rerun()

    # Cardinal stripe between the dark header and the content area.
    # The .app-header-stripe class lives in _GLOBAL_CSS; here we just
    # emit the element.
    st.markdown(
        '<div class="app-header-stripe"></div>',
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
