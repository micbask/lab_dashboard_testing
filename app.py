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

from config import DEFAULT_RESOURCES
from ui_components import inject_css, setup_mpl_font
import analytics.dashboard as _analytics
import pre_analytics.dashboard as _pre_analytics


# ═════════════════════════════════════════════════════════════════════════════
# FAVICON  (generated on first run; Pillow ships transitively with matplotlib
# which is in requirements.txt, so the import is safe)
# ═════════════════════════════════════════════════════════════════════════════
def _ensure_favicon(path: str = "assets/favicon.png") -> str | None:
    """Generate the bar-chart favicon PNG on disk if it isn't already there.

    The icon is a 64×64 transparent-background PNG with four ascending
    USC-cardinal (#790A26) bars. Returns the path on success, or None if
    Pillow isn't importable / the disk write fails (caller falls back to
    an emoji so st.set_page_config never receives a broken value).
    """
    if os.path.exists(path):
        return path
    try:
        from PIL import Image, ImageDraw
        os.makedirs(os.path.dirname(path), exist_ok=True)
        img = Image.new("RGBA", (64, 64), (0, 0, 0, 0))
        draw = ImageDraw.Draw(img)
        cardinal  = "#790A26"
        bar_w     = 9
        bar_gap   = 3
        bottom    = 56          # 8 px of bottom padding (bars span y=bottom-h..bottom-1)
        heights   = [18, 28, 38, 46]
        x = 10                   # (64 − (4*9 + 3*3)) // 2 ≈ 10
        for h in heights:
            draw.rectangle(
                [x, bottom - h, x + bar_w - 1, bottom - 1],
                fill=cardinal,
            )
            x += bar_w + bar_gap
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
    initial_sidebar_state="expanded",
)

# Streamlit Community Cloud appends "· Streamlit" to the document title
# after every rerun. Pin the title to "Lab Productivity" on script load
# and watch for any subsequent mutation so the suffix never sticks.
st.markdown(
    """
    <script>
    (function() {
        const desiredTitle = "Lab Productivity";
        document.title = desiredTitle;
        const observer = new MutationObserver(function() {
            if (document.title !== desiredTitle) {
                document.title = desiredTitle;
            }
        });
        const titleEl = document.querySelector('title');
        if (titleEl) {
            observer.observe(titleEl, { childList: true });
        }
    })();
    </script>
    """,
    unsafe_allow_html=True,
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

              /* Password input — override the global stTextInput rules. */
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"] input {
                  width: 100% !important;
                  padding: 11px 14px !important;
                  font-size: 14px !important;
                  font-weight: 400 !important;
                  border: 1px solid rgba(0, 0, 0, 0.14) !important;
                  border-radius: 8px !important;
                  background: #ffffff !important;
                  color: #1a1a1a !important;
                  box-sizing: border-box !important;
                  outline: none !important;
                  box-shadow: none !important;
                  font-family: 'Inter', system-ui, sans-serif !important;
              }
              html body #login-overlay [data-testid="stForm"]
                  [data-testid="stTextInput"] input:focus {
                  border-color: #790A26 !important;
                  outline: none !important;
                  box-shadow: 0 0 0 2px rgba(121, 10, 38, 0.12) !important;
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
                        <rect x="10" y="40" width="9" height="16" fill="#790A26"/>
                        <rect x="22" y="30" width="9" height="26" fill="#790A26"/>
                        <rect x="34" y="20" width="9" height="36" fill="#790A26"/>
                        <rect x="46" y="10" width="9" height="46" fill="#790A26"/>
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
