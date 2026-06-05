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

WHY st.markdown AND NOT st.components.v1.html:

An earlier version of this module rendered both surfaces in
``st.components.v1.html`` iframes so the Copy button could run real
JS (``navigator.clipboard.writeText``). That broke the more important
button: Streamlit's component iframe sandbox does NOT include
``allow-top-navigation`` (verified in
streamlit/static/.../IFrameUtil.BaqCY7QW.js — the sandbox is
``allow-forms allow-modals allow-popups
allow-popups-to-escape-sandbox allow-same-origin allow-scripts
allow-downloads``), so the embedded ``<a target="_top">`` is silently
blocked and the "Open the new dashboard" link did nothing. Since
the navigation button is the whole point of these surfaces, we
render via ``st.markdown(unsafe_allow_html=True)``.

WHY target="_top" IS EXPLICIT (do not change to _self or remove):

There are TWO layered defaults working against us; the markup has
to defeat both.

1. Streamlit's react-markdown anchor renderer (in src.D9MArGZj.js)
   defaults `target` to ``_blank`` for any anchor where the
   attribute isn't set:

        target: i || `_blank`

   So a plain ``<a href="...">`` becomes
   ``<a target="_blank">`` at render time and opens a new tab.
   Setting `target` explicitly defeats this.

2. Streamlit Cloud serves the app inside an outer iframe — the
   user's browser-tab URL is the iframe's HOST, not the app
   itself. ``target="_self"`` navigates the CURRENT frame (i.e.
   the inner iframe), which silently swaps the iframe content
   without updating the address bar. Symptom: the user clicks
   Open, sees the new app's UI, but the URL stays as the old
   *.streamlit.app host. When the new app then tries OAuth /
   Microsoft sign-in, the OAuth provider detects the framed
   context and pops the sign-in flow to a new tab as
   clickjacking protection — which is exactly what the user
   reported. ``target="_top"`` breaks out to the topmost browser
   window, so the URL bar actually updates and the new app
   loads at the top level the way it does on a direct visit.

`rel="noopener"` is added explicitly so the anchor doesn't fall
through to the renderer's default `rel: "noopener noreferrer"`
— `noopener` is enough for security on a same-tab navigation
and dropping `noreferrer` lets the destination see the referrer
(useful for the new app's analytics).

The Copy button is handled separately by ``st.code(URL)`` (native
Streamlit widget with a built-in one-click copy icon) on the login
welcome. The banner drops Copy entirely — the URL is visible in the
banner text and the Open button does the heavy lifting.

CSS is scoped under ``.labdash-welcome-scope`` / ``.labdash-banner-scope``
so the panel styling doesn't bleed into the rest of the Streamlit
page (no global ``*`` reset, no top-level ``:root`` overrides).
"""

from __future__ import annotations

import streamlit as st
from streamlit.components.v1 import html as _components_html


# Single source of truth for the destination so the copy + open + label
# can't drift apart.
NEW_APP_URL = "https://labdash.micbask.com"


# ═════════════════════════════════════════════════════════════════════════════
# OPEN-LINK NAVIGATION HELPER (JS, robust across hosting environments)
# ═════════════════════════════════════════════════════════════════════════════
# The markdown-rendered <a target="_top"> works in some environments and
# silently fails in others (e.g. when Streamlit Cloud serves the app
# inside an iframe whose sandbox blocks top navigation). To make the
# Open button RELIABLY do something — ideally same-tab nav, otherwise
# new-tab nav, but never nothing — we attach a JS click handler from
# inside a tiny invisible component iframe. The handler runs in the
# PARENT document context (where the anchor lives), so its navigation
# attempts honour the parent page's permissions rather than the
# component iframe's restrictive sandbox.
#
# Strategy in priority order:
#   1. window.top.location.href = url     — escape to topmost browser
#                                            window; the URL bar updates.
#   2. window.location.href = url         — fallback if (1) is blocked
#                                            (still same tab, but inside
#                                            whatever frame we're in).
#   3. window.open(url, "_blank")         — last resort. Opens new tab;
#                                            at least the user reaches
#                                            the destination.
#
# Wrapped in try/catch with a sentinel detection (`__labDashNavigated`)
# so we never hang on a silently-blocked nav.
def _open_link_navigator_html() -> str:
    return f"""<!doctype html><html><body><script>
(function() {{
  var url = {NEW_APP_URL!r};
  function wire() {{
    var doc;
    try {{ doc = window.parent.document; }} catch (e) {{ return; }}
    var anchors = doc.querySelectorAll('a.labdash-open-link');
    for (var i = 0; i < anchors.length; i++) {{
      var a = anchors[i];
      if (a.dataset.labdashWired === '1') continue;
      a.dataset.labdashWired = '1';
      a.addEventListener('click', function(ev) {{
        ev.preventDefault();
        var navigated = false;
        // 1. Try top-window navigation (same tab, URL bar updates).
        try {{
          window.parent.top.location.href = url;
          navigated = true;
        }} catch (err) {{}}
        // 2. Fallback: navigate the parent frame directly.
        if (!navigated) {{
          try {{
            window.parent.location.href = url;
            navigated = true;
          }} catch (err) {{}}
        }}
        // 3. Sentinel: if neither nav has taken effect after 250 ms,
        //    open in a new tab so the click never feels dead.
        setTimeout(function() {{
          try {{
            if (window.parent.location.href.indexOf(url) === -1) {{
              window.open(url, '_blank', 'noopener');
            }}
          }} catch (err) {{
            window.open(url, '_blank', 'noopener');
          }}
        }}, 250);
      }});
    }}
  }}
  // Anchors may not exist on first run (Streamlit renders async); poll
  // briefly. Once attached, the wired check above keeps us idempotent
  // across reruns.
  wire();
  var n = 0;
  var iv = setInterval(function() {{
    wire();
    if (++n > 40) clearInterval(iv);  // give up after ~10s
  }}, 250);
}})();
</script></body></html>"""


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

    The Open button is a real Streamlit-page anchor (no ``target``)
    so the click navigates the current tab to LabDash directly. The
    URL is also visible in the panel body (``labdash.micbask.com``)
    so users who want to copy it can select it from the text directly
    — no separate copy widget below the panel, which previously read
    as a stray code block sitting between the welcome card and the
    OR divider.
    """
    st.markdown(_welcome_panel_html(), unsafe_allow_html=True)
    # Invisible JS helper that intercepts clicks on .labdash-open-link
    # in the parent DOM and routes them through a top → parent → new-tab
    # navigation cascade. See _open_link_navigator_html for the
    # rationale (boils down to: Streamlit's markdown anchor with
    # target="_top" silently fails in some Cloud-hosted iframe
    # configurations and we need a JS fallback that always lands).
    _components_html(_open_link_navigator_html(), height=0)
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

    The Open button is a real Streamlit-page anchor (no ``target``)
    so the click navigates the current tab to LabDash directly.
    """
    st.markdown(_banner_html(), unsafe_allow_html=True)
    # Same JS helper as the login welcome — keeps Open behaviour
    # consistent on both surfaces. height=0 so the iframe doesn't
    # take vertical space.
    _components_html(_open_link_navigator_html(), height=0)
