"""
app.py — Lab Productivity Heatmap Dashboard (thin orchestrator)
Keck Medicine of USC

Handles: page config, auth gate, session-state init, dashboard routing.
All rendering logic lives in analytics/ and pre_analytics/ modules.
"""

import os
from copy import deepcopy

import streamlit as st
import streamlit.components.v1 as _components

from config import DEFAULT_RESOURCES
from ui_components import inject_css, inject_sidebar_resize_kill
import analytics.dashboard as _analytics
import pre_analytics.dashboard as _pre_analytics


# ═════════════════════════════════════════════════════════════════════════════
# FAVICON  (regenerated on every container start so disk + code never drift;
# Pillow ≥ 9.0 is pinned in requirements.txt — `rounded_rectangle` needs ≥ 8.2)
# ═════════════════════════════════════════════════════════════════════════════
def _ensure_favicon(path: str = "assets/favicon.png") -> str | None:
    """Generate the bar-chart favicon PNG.

    4 ascending bars in a 64×64 viewBox, all bottoms aligned at y=54 (small
    visual gap to the canvas edge), all bars 8 px wide with rounded top
    corners (rx=1.5). The first 3 bars are USC cardinal red (#790A26); the
    tallest (rightmost) bar is USC gold (#F1AB1F). Geometry matches the
    SVG used in the login overlay so favicon + in-app logo stay consistent.

    Rendered at 256×256 then LANCZOS-downscaled to 64×64 for crisp edges at
    every UI size — browsers downscale to 16/32 as needed. The function
    regenerates unconditionally (no `os.path.exists` short-circuit) so a
    stale PNG from a warm-restart container cannot mask a code change.
    Pillow's `rounded_rectangle` (added in 8.2) requires Pillow ≥ 9.0 which
    is pinned in requirements.txt. Returns the path on success, or None if
    Pillow is unimportable / disk write fails — caller falls back to an
    emoji so st.set_page_config never receives a broken value.
    """
    try:
        from PIL import Image, ImageDraw
        os.makedirs(os.path.dirname(path), exist_ok=True)
        # 4× supersample → crisp 64×64 after LANCZOS downscale.
        scale = 4
        size = 64 * scale
        img = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        cardinal = "#790A26"
        gold     = "#F1AB1F"
        # (x, y_top, width, height, fill) in 64×64 viewBox coords.
        bars = [
            (14, 36, 8, 18, cardinal),
            (26, 28, 8, 26, cardinal),
            (38, 20, 8, 34, cardinal),
            (50, 10, 8, 44, gold),
        ]
        for x, y, w, h, fill in bars:
            draw.rounded_rectangle(
                [x * scale, y * scale,
                 (x + w) * scale - 1, (y + h) * scale - 1],
                radius=int(1.5 * scale),
                fill=fill,
            )
        img = img.resize((64, 64), Image.LANCZOS)
        img.save(path, "PNG")
        return path
    except Exception:
        return None


_FAVICON = _ensure_favicon() or "🧪"


# ═════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG  (must be the first Streamlit call)
# ═════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Lab Productivity",
    page_icon=_FAVICON,
    layout="wide",
    initial_sidebar_state="auto",
)

# Pin browser-tab title to "Lab Productivity" with NO "· Streamlit"
# suffix. Per source inspection of streamlit/streamlit @ 1.57.0
# (PR #8900, June 2024 — removed the suffix from `getTitle()` in
# frontend/app/src/util/AppNavigation.ts), the suffix-appending code
# path no longer exists in the open-source frontend. If the user
# still observes the suffix on Cloud, the cause is one of:
#   • A stale browser tab title (cache, pinned tab, PWA, bookmark)
#   • A browser extension rewriting tab titles
#   • Closed-source Cloud-edge HTML rewriting (no documented case)
# This script is defense-in-depth against all of the above EXCEPT
# the last (which app-level JS cannot reach).
#
# Why an iframe (`components.v1.html`) and not `st.markdown(<script>)`:
# st.markdown routes through react-markdown + rehype-raw which renders
# <script> tags via React.createElement — React-created script
# elements DO NOT execute. `components.v1.html` embeds the markup via
# an <iframe srcdoc="..."> instead; scripts inside srcdoc iframes DO
# execute. The iframe sandbox is `allow-same-origin allow-scripts`
# (verified in frontend/lib/src/util/IFrameUtil.ts on 1.57.0), so
# `window.parent.document` is accessible.
#
# Three-layer defense against any title rewriting that runs after us:
#   1. SET `parent.document.title` immediately on iframe load.
#   2. MutationObserver on `parent.document.head` with
#      `childList + subtree + characterData` — catches text-node
#      mutation inside <title>, full <title> element replacement,
#      OR any other head-subtree mutation that could swap titles.
#      Spec: Document#title setter does "string replace all" on the
#      title element's children → fires `childList` on title node →
#      bubbles up via subtree:true on head.
#   3. setInterval (250 ms) polling fallback — catches anything the
#      observer somehow misses (browser quirks, async title writes,
#      etc.). At 4 Hz the CPU cost is negligible (~0.1%).
#
# Observer + interval dedup: each Streamlit rerun creates a fresh
# iframe with a fresh window. Without dedup we'd stack one observer
# and one interval per rerun. We store both on `parent.window` (which
# persists across iframe reloads) and explicitly disconnect / clear
# the prior instances before installing new ones.
_components.html(
    """
    <script>
    (function() {
        var desiredTitle = "Lab Productivity";
        try {
            var parentWin = window.parent;
            var parentDoc = parentWin.document;
            // Layer 1: immediate set.
            parentDoc.title = desiredTitle;
            // Layer 2: MutationObserver on <head> subtree.
            if (parentWin.__labTitleObserver) {
                try { parentWin.__labTitleObserver.disconnect(); } catch (e) {}
            }
            parentWin.__labTitleObserver = new MutationObserver(function() {
                if (parentDoc.title !== desiredTitle) {
                    parentDoc.title = desiredTitle;
                }
            });
            parentWin.__labTitleObserver.observe(parentDoc.head, {
                childList: true,
                subtree: true,
                characterData: true
            });
            // Layer 3: 250 ms polling fallback.
            if (parentWin.__labTitlePoller) {
                try { clearInterval(parentWin.__labTitlePoller); } catch (e) {}
            }
            parentWin.__labTitlePoller = setInterval(function() {
                if (parentDoc.title !== desiredTitle) {
                    parentDoc.title = desiredTitle;
                }
            }, 250);
        } catch (e) {
            // Cross-origin guard — defensive only; Streamlit's iframe
            // is `allow-same-origin allow-scripts` per IFrameUtil.ts
            // on 1.57.0 so we expect this path never runs.
        }
    })();
    </script>
    """,
    height=0,
)

inject_css()
inject_sidebar_resize_kill()


# ═════════════════════════════════════════════════════════════════════════════
# RETIREMENT LANDING (replaces the previous password gate)
# ═════════════════════════════════════════════════════════════════════════════
# This Streamlit app is retired: it has been superseded by the standalone
# LabDash web app at https://labdash.micbask.com. To shut off all access
# to the underlying analytics / pre-analytics dashboards while keeping the
# codebase intact for a possible future revert, we replaced the password
# gate with a one-page LabDash relocation landing followed by a hard
# st.stop().
#
# Everything below the stop — session-state init, dashboard routing,
# the retirement banner, the footer — is now unreachable. It's left in
# place rather than deleted so re-enabling the app is a one-line
# revert of this commit. The login-card CSS in the previous version is
# gone because the form it styled is gone; that's a clean drop, no
# dependants.
#
# Ingest pipeline (scripts/email_ingest.py + .github/workflows/xls-ingest.yml)
# is independent of this UI and continues to run. New partition data
# keeps landing in the repo so a revert wouldn't have a data gap.
from ui.labdash_notice import render_login_welcome

_, _retire_col, _ = st.columns([1, 1.5, 1])
with _retire_col:
    render_login_welcome()
st.stop()


# ═════════════════════════════════════════════════════════════════════════════
# Everything below is unreachable while the retirement landing is active.
# Kept in place so re-enabling the app is a one-commit revert.
# ═════════════════════════════════════════════════════════════════════════════


# ═════════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALISATION
# ═════════════════════════════════════════════════════════════════════════════
_ss = st.session_state

if "resource_assignments" not in _ss:
    _ss.resource_assignments = deepcopy(DEFAULT_RESOURCES)
if "last_map_type" not in _ss:
    _ss.last_map_type = None


# ═════════════════════════════════════════════════════════════════════════════
# RETIREMENT BANNER (dashboard only, post-auth)
# ═════════════════════════════════════════════════════════════════════════════
# Persistent notice pointing users at the standalone LabDash web app
# (labdash.micbask.com) that has replaced this Streamlit app. Mounted
# at the very top of the authenticated view so it shows on BOTH the
# analytics and pre-analytics dashboards. Dismissal persists per
# browser via localStorage inside the iframe. Does NOT appear on the
# pre-auth login screen (that path already returns via st.stop()
# above before reaching this block).
from ui.labdash_notice import render_dashboard_banner
render_dashboard_banner()


# ═════════════════════════════════════════════════════════════════════════════
# ROUTING
# ═════════════════════════════════════════════════════════════════════════════
# Nav is driven by st.button clicks in render_header that write to
# st.query_params and rerun. Mirror the URL into session state on every
# rerun (and fall back to "analytics" for direct visits).
_url_dashboard = st.query_params.get("dashboard")
if _url_dashboard in ("analytics", "pre_analytics"):
    _ss["_nav_dashboard"] = _url_dashboard
elif "_nav_dashboard" not in _ss:
    _ss["_nav_dashboard"] = "analytics"

_active_dashboard = _ss["_nav_dashboard"]

if _active_dashboard == "analytics":
    _params = _analytics.render_sidebar(_ss)
    _analytics.render(_params, _ss)
else:
    _params = _pre_analytics.render_sidebar(_ss)
    _pre_analytics.render(_params, _ss)


# ═════════════════════════════════════════════════════════════════════════════
# FOOTER  (rendered on both dashboards)
# Sits in normal document flow as the last block. The dashboard render()
# functions now `return` (rather than `st.stop()`) on no-data / empty
# states so this footer is reached on every view, including the TAT
# view, the "no data" welcome state, and the pre-analytics page.
# ═════════════════════════════════════════════════════════════════════════════
st.markdown(
    '<div style="text-align: center; font-size: 12px; '
    'color: rgba(0, 0, 0, 0.4); padding: 32px 0 16px 0;">'
    'v3.2 · May 2026'
    '</div>',
    unsafe_allow_html=True,
)
