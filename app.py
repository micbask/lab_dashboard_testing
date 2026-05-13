"""
app.py — Lab Productivity Heatmap Dashboard (thin orchestrator)
Keck Medicine of USC

Handles: page config, auth gate, session-state init, dashboard routing.
All rendering logic lives in analytics/ and pre_analytics/ modules.
"""

import os
from copy import deepcopy
from datetime import datetime

import streamlit as st
import streamlit.components.v1 as _components

from config import DEFAULT_RESOURCES
from ui_components import inject_css, setup_mpl_font
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
setup_mpl_font()


# ═════════════════════════════════════════════════════════════════════════════
# PASSWORD GATE
# ═════════════════════════════════════════════════════════════════════════════
_app_password = st.secrets.get("app_password", None)

if "app_authenticated" not in st.session_state:
    st.session_state["app_authenticated"] = False

if _app_password is not None and not st.session_state["app_authenticated"]:
    # Wrap in st.empty so we can instantly clear the overlay after auth
    # (avoids the multi-second lag while st.rerun() processes).
    _login_slot = st.empty()
    _auth_now = False
    with _login_slot.container():
        # ── Login-page CSS ───────────────────────────────────────────────────
        # The form wrapper itself IS the card. Streamlit's text input and
        # submit button get re-styled inside the card via overrides keyed
        # to #login-overlay so they don't leak into the rest of the app.
        st.markdown(
            """
            <style>
              /* Hide Streamlit's form-input hints (e.g. "Press Enter to
                 submit") that otherwise overlap the password show/hide
                 eye icon — both site-wide on the login page (no other
                 forms render here) and any nested form-instruction chip. */
              #login-overlay [data-testid="InputInstructions"],
              #login-overlay [data-testid="stFormHint"],
              [data-testid="InputInstructions"] {
                  display: none !important;
              }

              /* The form wrapper IS the card. */
              html body #login-overlay [data-testid="stForm"] {
                  width: 360px !important;
                  max-width: 360px !important;
                  padding: 40px 36px !important;
                  background: #ffffff !important;
                  border: 0.5px solid rgba(0, 0, 0, 0.08) !important;
                  border-radius: 12px !important;
                  box-sizing: border-box !important;
                  margin: 0 auto !important;
                  box-shadow: none !important;
              }

              /* Password input — wrapper-styled so the eye toggle sits
                 INSIDE the bordered box. Mirrors the corrected pattern
                 in ui_components.py. Targets the REAL baseweb DOM:
                 the MaskToggleButton is a sibling of <input> INSIDE
                 [data-baseweb="base-input"], NOT a direct child of
                 [data-baseweb="input"] (the earlier `> *:not(...)`
                 defensive rule matched nothing). Three layers:
                   (1) Root [data-baseweb="input"]: white bg + border.
                   (2) InputContainer [data-baseweb="base-input"]:
                       transparent so wrapper's white is the only
                       visible surface; zero padding so edges fill.
                   (3) <button> inside the wrapper: stripped chrome
                       and minimal padding (0 8px 0 4px) so the eye
                       SVG sits flush against the right border. */
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"] [data-baseweb="input"] {
                  width: 100% !important;
                  background: #ffffff !important;
                  border: 1px solid rgba(0, 0, 0, 0.14) !important;
                  border-radius: 8px !important;
                  padding: 0 !important;
                  box-sizing: border-box !important;
                  transition: border-color 0.15s ease, box-shadow 0.15s ease !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"] [data-baseweb="input"]:focus-within {
                  border-color: #790A26 !important;
                  box-shadow: 0 0 0 2px rgba(121, 10, 38, 0.12) !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"]
                  [data-baseweb="input"] [data-baseweb="base-input"] {
                  background: transparent !important;
                  background-color: transparent !important;
                  padding: 0 !important;
                  border: none !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"] [data-baseweb="input"] input {
                  width: 100% !important;
                  padding: 11px 14px !important;
                  padding-right: 32px !important;
                  font-size: 14px !important;
                  font-weight: 400 !important;
                  background: transparent !important;
                  border: none !important;
                  color: #1a1a1a !important;
                  box-sizing: border-box !important;
                  outline: none !important;
                  box-shadow: none !important;
                  font-family: 'Inter', system-ui, sans-serif !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"] [data-baseweb="input"] button {
                  background: transparent !important;
                  background-color: transparent !important;
                  border: none !important;
                  box-shadow: none !important;
                  outline: none !important;
                  padding: 0 8px 0 4px !important;
                  margin: 0 !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"] [data-baseweb="input"] button svg {
                  margin: 0 !important;
                  padding: 0 !important;
                  display: block !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"] {
                  margin-bottom: 12px !important;
              }

              /* Sign-in button — override the maroon site-wide button rule
                 with the login-page primary color (#790A26) and the
                 spec'd geometry. */
              html body #login-overlay [data-testid="stForm"] .stButton > button,
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stFormSubmitButton"] button,
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stBaseButton-primaryFormSubmit"],
              html body #login-overlay [data-testid="stForm"]
                  button[kind="primaryFormSubmit"] {
                  width: 100% !important;
                  padding: 11px 16px !important;
                  font-size: 14px !important;
                  font-weight: 500 !important;
                  color: #ffffff !important;
                  background: #790A26 !important;
                  background-color: #790A26 !important;
                  border: none !important;
                  border-radius: 8px !important;
                  cursor: pointer !important;
                  text-shadow: none !important;
                  box-shadow: none !important;
                  font-family: 'Inter', system-ui, sans-serif !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stFormSubmitButton"] button:hover,
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stBaseButton-primaryFormSubmit"]:hover {
                  background: #5e0820 !important;
                  background-color: #5e0820 !important;
                  border: none !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stFormSubmitButton"] button p,
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stFormSubmitButton"] button div {
                  color: #ffffff !important;
                  font-weight: 500 !important;
                  font-size: 14px !important;
                  text-shadow: none !important;
              }

              /* Bar-chart icon + title + subtitle inside the card. */
              .login-icon-block { text-align: center; }
              .login-icon-block svg {
                  display: block;
                  margin: 0 auto 20px auto;
                  width: 34px;
                  height: 34px;
              }
              .login-icon-block .login-title {
                  font-size: 19px;
                  font-weight: 500;
                  color: #1a1a1a;
                  text-align: center;
                  line-height: 1.3;
                  margin: 0 0 6px 0;
                  font-family: 'Inter', system-ui, sans-serif;
              }
              .login-icon-block .login-subtitle {
                  font-size: 11px;
                  font-weight: 500;
                  color: #C9941A;
                  letter-spacing: 0.12em;
                  text-align: center;
                  margin: 0 0 32px 0;
                  font-family: 'Inter', system-ui, sans-serif;
              }

              /* Footer underneath the card. */
              .login-footer {
                  text-align: center;
                  font-size: 12px;
                  color: rgba(0, 0, 0, 0.45);
                  margin-top: 32px;
                  font-family: 'Inter', system-ui, sans-serif;
              }
            </style>
            """,
            unsafe_allow_html=True,
        )

        # Push the card down a bit from the top of the viewport so it
        # appears centered without using flex (which would conflict
        # with Streamlit's default block layout).
        st.markdown(
            '<div id="login-overlay" style="padding-top: 10vh;">',
            unsafe_allow_html=True,
        )
        _, col, _ = st.columns([1, 0.9, 1])
        with col:
            with st.form("login_form", enter_to_submit=True):
                st.markdown(
                    """
                    <div class="login-icon-block">
                      <svg viewBox="0 0 64 64" xmlns="http://www.w3.org/2000/svg">
                        <rect x="14" y="36" width="8" height="18" fill="#790A26" rx="1.5"/>
                        <rect x="26" y="28" width="8" height="26" fill="#790A26" rx="1.5"/>
                        <rect x="38" y="20" width="8" height="34" fill="#790A26" rx="1.5"/>
                        <rect x="50" y="10" width="8" height="44" fill="#F1AB1F" rx="1.5"/>
                      </svg>
                      <div class="login-title">Laboratory Productivity Dashboard</div>
                      <div class="login-subtitle">ANALYTICS &nbsp;·&nbsp; PRE-ANALYTICS</div>
                    </div>
                    """,
                    unsafe_allow_html=True,
                )
                password = st.text_input(
                    "Password", type="password",
                    label_visibility="collapsed",
                    placeholder="Password",
                )
                submitted = st.form_submit_button("Sign in", width="stretch")
                if submitted:
                    if password == st.secrets.get("app_password", ""):
                        st.session_state["app_authenticated"] = True
                        _auth_now = True
                    else:
                        st.error("Incorrect password. Please try again.")

            # Footer beneath the card (stays inside the centered column).
            st.markdown(
                f'<div class="login-footer">'
                f'© {datetime.now().year} Laboratory Productivity Dashboard. '
                f'All rights reserved.'
                f'</div>',
                unsafe_allow_html=True,
            )

        st.markdown('</div>', unsafe_allow_html=True)
    if _auth_now:
        _login_slot.empty()  # instantly clear the overlay before rerun
        st.rerun()
    st.stop()


# ═════════════════════════════════════════════════════════════════════════════
# SESSION STATE INITIALISATION
# ═════════════════════════════════════════════════════════════════════════════
_ss = st.session_state

if "resource_assignments" not in _ss:
    _ss.resource_assignments = deepcopy(DEFAULT_RESOURCES)
if "last_map_type" not in _ss:
    _ss.last_map_type = None


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
    f'<div style="text-align: center; font-size: 12px; '
    f'color: rgba(0, 0, 0, 0.4); padding: 32px 0 16px 0;">'
    f'© {datetime.now().year} Laboratory Productivity Dashboard. '
    f'All rights reserved.'
    f'</div>',
    unsafe_allow_html=True,
)
