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
        # Hide Streamlit's "Press Enter to submit" hint inside the login
        # form (it overlaps the password show/hide eye icon) and the
        # "Press Enter to apply" InputInstructions chip.
        st.markdown(
            """
            <style>
              #login-overlay [data-testid="InputInstructions"],
              #login-overlay div[data-testid="stFormSubmitButton"] + div,
              #login-overlay [data-testid="stFormHint"] {
                  display: none !important;
              }
            </style>
            """,
            unsafe_allow_html=True,
        )
        st.markdown('<div id="login-overlay">', unsafe_allow_html=True)
        st.markdown('<div style="height: 15vh; min-height: 48px;"></div>',
                    unsafe_allow_html=True)
        _, col, _ = st.columns([1, 0.9, 1])
        with col:
            # Banner matches the in-app banner: same maroon gradient,
            # padding, border-radius, shadow, and gold subtitle styling.
            # CLINICAL LAB pill removed; title now reads
            # "Laboratory Productivity Dashboard"; subtitle
            # "Analytics · Pre-Analytics" advertises both views.
            st.markdown(
                """
                <div style="
                    background: linear-gradient(135deg, #6F1828 0%, #521322 100%);
                    padding: 1.2rem 1.8rem;
                    border-radius: 10px 10px 0 0;
                    text-align: center;
                    box-shadow: 0 2px 8px rgba(111,24,40,0.25);
                ">
                    <h1 style="
                        color: #ffffff;
                        font-family: 'Inter', system-ui, sans-serif;
                        font-size: 1.5rem;
                        font-weight: 700;
                        letter-spacing: 0.2px;
                        margin: 0;
                        padding: 0;
                        line-height: 1.2;
                    ">Laboratory Productivity Dashboard</h1>
                    <p style="
                        color: #EDC153;
                        font-family: 'Inter', system-ui, sans-serif;
                        font-size: 0.87rem;
                        font-weight: 500;
                        margin: 0.2rem 0 0 0;
                        padding: 0;
                        opacity: 0.95;
                        line-height: 1.2;
                    ">Analytics &nbsp;·&nbsp; Pre-Analytics</p>
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
                """,
                unsafe_allow_html=True,
            )
            with st.form("login_form", enter_to_submit=True):
                password = st.text_input(
                    "Password", type="password",
                    label_visibility="collapsed",
                    placeholder="Password",
                )
                submitted = st.form_submit_button("Sign In", width="stretch")
                if submitted:
                    if password == st.secrets.get("app_password", ""):
                        st.session_state["app_authenticated"] = True
                        _auth_now = True
                    else:
                        st.error("Incorrect password. Please try again.")
            st.markdown(
                f"""
                </div>
                <div style="text-align: center; font-size: 12px;
                            color: rgba(0, 0, 0, 0.4);
                            padding: 32px 0 16px 0;
                            font-family: 'Inter', system-ui, sans-serif;">
                    © {datetime.now().year} Laboratory Productivity Dashboard.
                    All rights reserved.
                </div>
                """,
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
