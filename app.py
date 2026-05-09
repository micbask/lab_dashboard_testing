"""
app.py — Lab Productivity Heatmap Dashboard (thin orchestrator)
Keck Medicine of USC

Handles: page config, auth gate, session-state init, dashboard routing.
All rendering logic lives in analytics/ and pre_analytics/ modules.
"""

from copy import deepcopy

import streamlit as st

from config import DEFAULT_RESOURCES
from ui_components import inject_css, setup_mpl_font
import analytics.dashboard as _analytics
import pre_analytics.dashboard as _pre_analytics


# ═════════════════════════════════════════════════════════════════════════════
# PAGE CONFIG  (must be the first Streamlit call)
# ═════════════════════════════════════════════════════════════════════════════
st.set_page_config(
    page_title="Lab Productivity · Keck Medicine",
    page_icon="🧪",
    layout="wide",
    initial_sidebar_state="expanded",
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
        st.markdown('<div id="login-overlay">', unsafe_allow_html=True)
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
                    ">CLINICAL LAB</div>
                    <div style="
                        color: #ffffff;
                        font-family: 'Inter', system-ui, sans-serif;
                        font-size: 1.45rem;
                        font-weight: 700;
                        letter-spacing: 0.01em;
                        margin: 0 0 0.3rem 0;
                        line-height: 1.2;
                    ">Productivity Dashboard</div>
                    <div style="
                        color: rgba(237,193,83,0.85);
                        font-family: 'Inter', system-ui, sans-serif;
                        font-size: 0.82rem;
                        font-weight: 400;
                        letter-spacing: 0.01em;
                        margin: 0;
                    ">Analytics</div>
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
                submitted = st.form_submit_button("Sign In", width="stretch")
                if submitted:
                    if password == st.secrets.get("app_password", ""):
                        st.session_state["app_authenticated"] = True
                        _auth_now = True
                    else:
                        st.error("Incorrect password. Please try again.")
            st.markdown("""
                </div>
                <div style="text-align:center; margin-top: 1.2rem; color: #94a3b8; font-size: 0.7rem; font-family: 'Inter', system-ui, sans-serif;">
                    Laboratory &nbsp;·&nbsp; Dashboard
                </div>
            """, unsafe_allow_html=True)
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
# Nav is driven by HTML anchor tags in render_header that set ?dashboard=…
# on click, so the URL is the source of truth. Mirror the URL into session
# state on every rerun (and fall back to "analytics" for direct visits).
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
