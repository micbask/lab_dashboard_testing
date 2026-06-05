"""
ui/labdash_notice.py — Relocation / retirement notices pointing users
to the standalone LabDash web app at labdash.micbask.com.

This Streamlit app has been superseded by a separate web app
(branded "LabDash") on a different stack. The Streamlit app is being
kept running as a fallback for users (notably @med.usc.edu accounts)
who cannot yet sign in to the new app. Two surfaces here:

  render_login_welcome()      — large welcome panel rendered ABOVE
                                the native password gate on the
                                pre-auth login screen.

  render_dashboard_banner()   — slim retirement banner mounted at
                                the very top of the authenticated
                                dashboard (analytics + pre-analytics).

HOW THE OPEN BUTTON ACTUALLY NAVIGATES (read this before changing it):

Two findings from direct research (Streamlit forum + github issues +
reading frontend/lib/src/util/IFrameUtil.ts on streamlit/develop):

  • Apps deployed at ``*.streamlit.app`` render at the TOP LEVEL of
    the browser tab. There is no outer wrapping iframe.
  • The iframe sandbox that blocks top navigation is applied ONLY
    to ``st.components.v1.html`` iframes. That sandbox omits
    ``allow-top-navigation``, so from inside a component iframe
    ``target="_top"`` and ``window.top.location.href = url`` both
    silently fail and any navigation falls through to
    ``window.open(_blank)`` — a new tab.

So the navigation must happen from app-level (top-level) code, not
from inside a component iframe. The previous round mistakenly used
``st.components.v1.html`` for the navigation helper and hit exactly
that sandbox — new tab every time.

We can't reliably use a plain HTML anchor either:

  1. Streamlit's react-markdown anchor renderer (NT in
     src.D9MArGZj.js) forces ``target="_blank"`` on any anchor
     where the attribute isn't preserved through sanitization, and
  2. The sanitizer used by both ``st.markdown`` and ``st.html``
     strips ``target`` values other than ``_blank`` (issues #4346
     and #9972 in streamlit/streamlit).

The combination means a plain ``<a target="_top">`` ends up as
``<a target="_blank">`` and opens in a new tab no matter what.

The working primitive is ``st.html(script, unsafe_allow_javascript=True)``.
Per the docstring: "st.html content is not iframed", and with the
JS flag the embedded script actually executes in the top-level
document. From there, ``window.location.href = url`` navigates the
browser tab directly. The URL bar updates and the new app's OAuth
behaves the same as a direct visit (no enclosing frame ⇒ no
clickjacking countermeasure ⇒ no new-tab pop-out).

The visible button is still an ``<a class="cta labdash-open-link">``
rendered by ``st.markdown`` so the cardinal-CTA styling stays in
the panel layout. The script finds every ``.labdash-open-link``
anchor, preventDefault's the click (cancelling the markdown
renderer's auto-added ``target="_blank"``), and assigns
``window.location.href``. Idempotent across Streamlit reruns via a
``data-labdash-wired`` marker. See ``_open_link_top_nav_script``.
"""

from __future__ import annotations

import streamlit as st


# Single source of truth for the destination so the copy + open + label
# can't drift apart.
NEW_APP_URL = "https://labdash.micbask.com"


# ═════════════════════════════════════════════════════════════════════════════
# OPEN-LINK NAVIGATION HELPER (top-level JS via st.html)
# ═════════════════════════════════════════════════════════════════════════════
# The previous round's component-iframe helper opened a new tab. Root
# cause traced by direct research:
#
#   • Streamlit apps deployed at *.streamlit.app render at the TOP
#     LEVEL of the browser tab — there is NO outer wrapping iframe.
#     (Verified against streamlit's IFrameUtil.ts source.)
#   • The sandbox that blocks top-window navigation is ONLY applied
#     to `st.components.v1.html` iframes. From inside a component
#     iframe, `target="_top"` and `window.top.location.href = url`
#     are silently dropped, so any navigation attempt there falls
#     through to `window.open(url, "_blank")` — a new tab.
#   • Streamlit's react-markdown anchor renderer (NT in
#     src.D9MArGZj.js) ALSO forces `target="_blank"` on any anchor
#     where the attribute isn't preserved through sanitization, so
#     a plain `<a target="_top">` written via `st.markdown` ends up
#     as `_blank` regardless.
#
# The right primitive is `st.html(unsafe_allow_javascript=True)`:
# it renders inline at the app's top level (NOT iframed, per the
# docstring: "st.html content is not iframed") and lets the embedded
# script actually run in the top-level document. From there,
# `window.location.href = url` navigates the actual browser tab.
# The URL bar updates. No new tab. OAuth on the new app behaves the
# same as a direct visit because there's no enclosing frame.
def _open_link_top_nav_script() -> str:
    """Tiny <script> rendered via st.html(unsafe_allow_javascript=True)
    that wires every `.labdash-open-link` anchor in the page to
    navigate the current tab via `window.location.href`. Renders at
    the app's top level so the navigation actually affects the
    browser-tab URL.
    """
    return f"""<script>
(function() {{
  var url = {NEW_APP_URL!r};
  function wire() {{
    var anchors = document.querySelectorAll(
      'a.labdash-open-link:not([data-labdash-wired])'
    );
    anchors.forEach(function(a) {{
      a.dataset.labdashWired = '1';
      a.addEventListener('click', function(ev) {{
        ev.preventDefault();
        // Top-level same-tab navigation — the URL bar updates and
        // the destination loads at the top level the way it does
        // on a direct visit.
        window.location.href = url;
      }});
    }});
  }}
  wire();
  // Streamlit re-renders the DOM on every interaction; rewire when
  // new anchors appear. Cheap (querySelectorAll on a small page).
  setInterval(wire, 500);
}})();
</script>"""


# 3x3 grid mark, deep red ramping to gold top-right cell. Same SVG the
# standalone LabDash site uses so the brand reads consistently across
# the relocation surfaces.
def _labdash_mark_svg(width: int, height: int) -> str:
    return (
        f'<svg width="{width}" height="{height}" viewBox="0 0 32 32" '
        'fill="none" role="img" aria-label="LabDash">'
        '<rect x="2"  y="2"  width="8" height="8" rx="2.5" fill="#AE1F22"/>'
        '<rect x="12" y="2"  width="8" height="8" rx="2.5" fill="#7A0A1C"/>'
        '<rect x="22" y="2"  width="8" height="8" rx="2.5" fill="#FFCC00"/>'
        '<rect x="2"  y="12" width="8" height="8" rx="2.5" fill="#D96A6A"/>'
        '<rect x="12" y="12" width="8" height="8" rx="2.5" fill="#AE1F22"/>'
        '<rect x="22" y="12" width="8" height="8" rx="2.5" fill="#7A0A1C"/>'
        '<rect x="2"  y="22" width="8" height="8" rx="2.5" fill="#F5CACA"/>'
        '<rect x="12" y="22" width="8" height="8" rx="2.5" fill="#D96A6A"/>'
        '<rect x="22" y="22" width="8" height="8" rx="2.5" fill="#AE1F22"/>'
        '</svg>'
    )


# Google Fonts import — kept at the very top of each <style> block so
# the @import resolves before subsequent rules reference the families.
# Loads page-globally (fonts are inert until something uses them via
# `font-family`), so we don't worry about scoping.
_FONT_IMPORT = (
    "@import url('https://fonts.googleapis.com/css2?"
    "family=Fraunces:ital,opsz,wght@0,9..144,400;0,9..144,500;"
    "0,9..144,600;1,9..144,500"
    "&family=Geist:wght@400;500;600"
    "&family=Geist+Mono:wght@400;500"
    "&display=swap');"
)


# ═════════════════════════════════════════════════════════════════════════════
# LOGIN WELCOME PANEL
# ═════════════════════════════════════════════════════════════════════════════

def _welcome_panel_html() -> str:
    """Full <style>+<div> block for the login welcome panel.

    Scoped under ``.labdash-welcome-scope`` so descendants get the
    brand tokens + locally-namespaced typography without affecting
    the rest of the Streamlit page. A local box-sizing reset is
    applied to descendants only; no global ``*`` selector.
    """
    return f"""
<style>
{_FONT_IMPORT}

.labdash-welcome-scope {{
  --cardinal:#990000; --cardinal-deep:#7A0A1C;
  --gold:#C2870F; --peak:#FFCC00;
  --surface:#FFFFFF; --ink:#1A1613; --body:#3C352E;
  --muted:#6E665E; --soft:#9A938A;
  --line:#E8E2D9; --line-soft:#F1ECE4;

  /* Width mechanism mirrors the login form EXACTLY (the form uses
     `width: 480px !important; max-width: 480px !important`). A HARD
     width holds 480px regardless of the column's actual width;
     `max-width` alone — or a `max-width: 100%` guard — collapses to
     a narrower parent, which is precisely what made this panel
     shrink to ~360px while the hard-width form stayed 480px. Same
     mechanism on both → identical width, same centre axis. Centred
     with margin:0 auto inside the shared st.columns wrapper. */
  width:480px;max-width:480px;margin:0 auto 4px auto;padding:0;
  font-family:"Geist",system-ui,-apple-system,"Segoe UI",sans-serif;
  color:var(--ink);
  -webkit-font-smoothing:antialiased;
  text-rendering:optimizeLegibility;
}}
.labdash-welcome-scope *,
.labdash-welcome-scope *::before,
.labdash-welcome-scope *::after {{
  box-sizing:border-box;
}}
.labdash-welcome-scope .panel {{
  position:relative;background:var(--surface);
  border:1px solid var(--line);border-radius:14px;
  padding:clamp(20px,4vw,28px);
  box-shadow:0 1px 2px rgba(26,22,19,.04),
             0 26px 64px -30px rgba(26,22,19,.20);
  overflow:hidden;
}}
.labdash-welcome-scope .panel::before {{
  content:"";position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,var(--peak) 0 40px,var(--cardinal) 40px 100%);
}}
.labdash-welcome-scope .eyebrow {{
  display:inline-flex;align-items:center;gap:8px;
  font-family:"Geist Mono",ui-monospace,monospace;
  font-size:10.5px;letter-spacing:.18em;text-transform:uppercase;
  color:var(--cardinal);font-weight:500;margin-bottom:16px;
}}
.labdash-welcome-scope .eyebrow .dot {{
  width:6px;height:6px;border-radius:50%;background:var(--cardinal);
}}
.labdash-welcome-scope .lockup {{
  display:flex;align-items:center;gap:11px;margin-bottom:18px;
}}
.labdash-welcome-scope .lockup .word {{
  font-family:"Fraunces",Georgia,serif;font-weight:600;
  font-size:18px;letter-spacing:-.01em;color:var(--ink);line-height:1.1;
  margin:0;
}}
.labdash-welcome-scope .lockup .formerly {{
  font-family:"Geist Mono",ui-monospace,monospace;
  font-size:10px;color:var(--soft);margin-top:3px;line-height:1.35;
}}
.labdash-welcome-scope .panel h1 {{
  font-family:"Fraunces",Georgia,serif;font-weight:600;
  font-size:clamp(1.5rem,3.5vw,1.85rem);line-height:1.1;
  letter-spacing:-.01em;color:var(--ink);
  margin:0 0 14px 0;padding:0;
}}
.labdash-welcome-scope .msg p {{
  font-size:.95rem;line-height:1.55;
  color:var(--body);margin:0;padding:0;
}}
.labdash-welcome-scope .msg p + p {{ margin-top:10px; }}
.labdash-welcome-scope .msg .inline-url {{
  font-family:"Geist Mono",ui-monospace,monospace;
  font-weight:500;color:var(--cardinal);
}}
.labdash-welcome-scope .msg .sig {{
  margin-top:14px;font-family:"Fraunces",Georgia,serif;
  font-style:italic;font-weight:500;font-size:1rem;color:var(--ink);
}}
.labdash-welcome-scope .actions {{
  display:flex;flex-wrap:wrap;gap:10px;align-items:center;margin-top:20px;
}}
.labdash-welcome-scope .cta {{
  display:inline-flex;align-items:center;gap:9px;
  background:var(--cardinal) !important;color:#fff !important;
  text-decoration:none !important;
  font-family:"Geist",system-ui,sans-serif;font-weight:600;font-size:.95rem;
  padding:11px 18px;border-radius:10px;
  transition:transform .15s ease,background .15s ease,box-shadow .15s ease;
  box-shadow:0 1px 2px rgba(122,10,28,.18),0 12px 24px -14px rgba(122,10,28,.5);
}}
.labdash-welcome-scope .cta:hover {{
  background:var(--cardinal-deep) !important;transform:translateY(-1px);
}}
.labdash-welcome-scope .cta .arrow {{ transition:transform .15s ease; }}
.labdash-welcome-scope .cta:hover .arrow {{ transform:translateX(3px); }}
</style>
<div class="labdash-welcome-scope">
  <main class="panel" role="main">
    <div class="eyebrow"><span class="dot"></span>We&rsquo;ve moved</div>
    <div class="lockup">
      {_labdash_mark_svg(36, 36)}
      <div>
        <div class="word">LabDash</div>
        <div class="formerly">formerly the Laboratory Productivity Dashboard</div>
      </div>
    </div>
    <h1>LabDash has a new home</h1>
    <div class="msg">
      <p>Hi team, we&rsquo;ve rebuilt the dashboard as its own standalone app, so it is
         faster, more secure, and ready for new features. The new home is
         <span class="inline-url">labdash.micbask.com</span>.</p>
      <p>If you run into any access issues, reach out to the Ops team or me and we&rsquo;ll
         get it sorted. Thanks!</p>
      <p class="sig">Michael</p>
    </div>
    <div class="actions">
      <a class="cta labdash-open-link" data-labdash-href="{NEW_APP_URL}"
         href="{NEW_APP_URL}" target="_top" rel="noopener">
        Open the new dashboard <span class="arrow" aria-hidden="true">&rarr;</span>
      </a>
    </div>
  </main>
</div>
"""


# CSS-only "OR" divider + fallback lead-in between the welcome panel
# and the existing native password form below.
_LOGIN_OR_DIVIDER_HTML = """
<style>
.labdash-or-wrap{
  width:480px;max-width:480px;margin:22px auto 18px auto;padding:0;
  font-family:"Geist",system-ui,-apple-system,"Segoe UI",sans-serif;
}
.labdash-or-divider{
  display:flex;align-items:center;gap:12px;color:#9A938A;margin:0 2px 18px 2px;
}
.labdash-or-divider::before,.labdash-or-divider::after{
  content:"";height:1px;flex:1;background:#E8E2D9;
}
.labdash-or-divider span{
  font-family:"Geist Mono",ui-monospace,SFMono-Regular,monospace;
  font-size:10px;letter-spacing:.16em;text-transform:uppercase;
}
.labdash-fallback-note{
  text-align:center;font-size:.84rem;color:#6E665E;line-height:1.45;
  margin:0 0 18px 0;
}
</style>
<div class="labdash-or-wrap">
  <div class="labdash-or-divider"><span>or</span></div>
  <p class="labdash-fallback-note">
    Still need this version? Enter the password to continue.
  </p>
</div>
"""


def render_login_welcome() -> None:
    """Render the welcome panel + OR divider + fallback lead-in above
    the native password form on the pre-auth login screen.

    The Open button's click is wired by a small JS block injected
    via ``st.html(unsafe_allow_javascript=True)``. That call renders
    at the app's TOP LEVEL (per Streamlit docs: "st.html content is
    not iframed") so ``window.location.href = url`` actually
    navigates the browser tab, updates the URL bar, and lets the
    new app's OAuth complete in-place. See
    ``_open_link_top_nav_script`` for the rationale.
    """
    st.markdown(_welcome_panel_html(), unsafe_allow_html=True)
    st.html(_open_link_top_nav_script(), unsafe_allow_javascript=True)
    st.markdown(_LOGIN_OR_DIVIDER_HTML, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# DASHBOARD RETIREMENT BANNER
# ═════════════════════════════════════════════════════════════════════════════

def _banner_html() -> str:
    """Full <style>+<div> block for the dashboard retirement banner.

    The URL is in the banner text and the Open button is the primary
    action; Copy is dropped (an additional st.code right under the
    banner would visually disrupt the dashboard layout). Dismiss is
    also dropped — the banner is a persistent "we WANT you to see
    this" notice. Both omissions were called out as acceptable in
    the original design brief.
    """
    return f"""
<style>
{_FONT_IMPORT}

.labdash-banner-scope {{
  --cardinal:#990000; --cardinal-deep:#7A0A1C;
  --gold:#C2870F;
  --ink:#1A1613;

  margin:0 0 8px 0;
  font-family:"Geist",system-ui,-apple-system,"Segoe UI",sans-serif;
  color:var(--ink);
  -webkit-font-smoothing:antialiased;
}}
.labdash-banner-scope *,
.labdash-banner-scope *::before,
.labdash-banner-scope *::after {{
  box-sizing:border-box;
}}
.labdash-banner-scope .depbar {{
  display:flex;align-items:center;gap:16px;flex-wrap:wrap;
  background:linear-gradient(180deg,#FCF5E2,#FBF1D8);
  border:1px solid #ECD9A6;border-radius:10px;
  box-shadow:0 6px 18px -12px rgba(122,60,0,.30);
  padding:11px 18px;
}}
.labdash-banner-scope .depbar .icon {{
  flex:none;display:flex;
}}
.labdash-banner-scope .depbar .txt {{
  font-size:.92rem;line-height:1.4;color:var(--ink);min-width:240px;
}}
.labdash-banner-scope .depbar .txt .mono {{
  font-family:"Geist Mono",ui-monospace,monospace;
  font-weight:500;color:var(--cardinal);
}}
.labdash-banner-scope .depbar .txt strong {{ font-weight:600; }}
.labdash-banner-scope .depbar .txt .note {{
  display:block;color:#7a6a42;font-size:.78rem;margin-top:2px;
}}
.labdash-banner-scope .depbar .spacer {{ flex:1 1 12px; }}
.labdash-banner-scope .depbar .row {{
  display:flex;align-items:center;gap:9px;
}}
.labdash-banner-scope .bb {{
  display:inline-flex;align-items:center;gap:8px;
  font-family:"Geist",system-ui,sans-serif;font-weight:600;font-size:.9rem;
  padding:9px 15px;border-radius:9px;cursor:pointer;
  border:1px solid transparent;
  text-decoration:none !important;
  transition:transform .14s ease,background .14s ease,
             border-color .14s ease,color .14s ease,box-shadow .14s ease;
}}
.labdash-banner-scope .bb-primary {{
  background:var(--cardinal) !important;color:#fff !important;
  box-shadow:0 1px 2px rgba(122,10,28,.18),
             0 10px 20px -14px rgba(122,10,28,.55);
}}
.labdash-banner-scope .bb-primary:hover {{
  background:var(--cardinal-deep) !important;
  transform:translateY(-1px);
}}
.labdash-banner-scope .bb-primary .arrow {{ transition:transform .14s ease; }}
.labdash-banner-scope .bb-primary:hover .arrow {{ transform:translateX(2px); }}
</style>
<div class="labdash-banner-scope">
  <div class="depbar" role="region" aria-label="Service notice">
    <span class="icon">{_labdash_mark_svg(26, 26)}</span>
    <div class="txt">
      LabDash has moved to <span class="mono">labdash.micbask.com</span>.
      This version will be retired <strong>soon</strong>, please switch over when you can.
      <span class="note">Sign in on the new site with your @usc.edu Microsoft account.</span>
    </div>
    <span class="spacer"></span>
    <div class="row">
      <a class="bb bb-primary labdash-open-link" data-labdash-href="{NEW_APP_URL}"
         href="{NEW_APP_URL}" target="_top" rel="noopener">
        Open new dashboard <span class="arrow" aria-hidden="true">&rarr;</span>
      </a>
    </div>
  </div>
</div>
"""


def render_dashboard_banner() -> None:
    """Render the retirement banner at the top of the authenticated
    dashboard (called once on every rerun, on both analytics and
    pre-analytics dashboards).

    Open-link wiring matches render_login_welcome — JS injected via
    ``st.html(unsafe_allow_javascript=True)`` runs at the app's top
    level and navigates the browser tab via ``window.location.href``.
    """
    st.markdown(_banner_html(), unsafe_allow_html=True)
    st.html(_open_link_top_nav_script(), unsafe_allow_javascript=True)
