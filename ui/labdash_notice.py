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

Both surfaces use ``st.components.v1.html`` (an iframe) rather than
``st.markdown`` because the Copy button requires real JS
(``navigator.clipboard.writeText`` + execCommand fallback) — markdown
strips scripts. Inside the iframe, the Open link uses
``target="_top"`` so clicking it navigates the top window instead of
the embedded iframe.

The banner dismiss is wired through ``localStorage`` rather than
``st.session_state``. A session_state round-trip would need a
Streamlit-side button outside the iframe to communicate the dismiss
back to Python, which would visually clash with the banner. Using
localStorage gives a clean self-contained dismiss that persists
across Streamlit reruns AND across reloads in the same browser.
"""

from __future__ import annotations

import streamlit as st
from streamlit.components.v1 import html as _components_html


# Single source of truth for the destination so the copy + open + label
# can't drift apart. Mirrors the value baked into the iframe HTML below.
NEW_APP_URL = "https://labdash.micbask.com"


# ═════════════════════════════════════════════════════════════════════════════
# SHARED STYLES
# ═════════════════════════════════════════════════════════════════════════════
# Brand tokens lifted verbatim from the design mock so the two iframes
# render consistently with each other and with the standalone LabDash
# site. Fonts are pulled from Google Fonts; the iframe is sandboxed
# from the parent page so this @import is scoped to the notice
# surfaces and doesn't leak into the rest of the Streamlit app.
_FONT_IMPORT = (
    "@import url('https://fonts.googleapis.com/css2?"
    "family=Fraunces:ital,opsz,wght@0,9..144,400;0,9..144,500;"
    "0,9..144,600;1,9..144,500"
    "&family=Geist:wght@400;500;600"
    "&family=Geist+Mono:wght@400;500"
    "&display=swap');"
)

_BRAND_TOKENS = """
:root{
  --cardinal:#990000; --cardinal-deep:#7A0A1C;
  --gold:#C2870F; --peak:#FFCC00;
  --bg:#FAF8F4; --surface:#FFFFFF;
  --ink:#1A1613; --body:#3C352E;
  --muted:#6E665E; --soft:#9A938A;
  --line:#E8E2D9; --line-soft:#F1ECE4;
}
*{box-sizing:border-box;margin:0;padding:0;}
html,body{background:transparent;}
"""

# 3x3 grid mark, deep red ramping to gold top-right cell. The exact
# same SVG used by the standalone LabDash site so the brand reads
# consistently across the relocation surfaces.
_LABDASH_MARK_SVG = """\
<svg width="{w}" height="{h}" viewBox="0 0 32 32" fill="none" role="img" aria-label="LabDash">
  <rect x="2"  y="2"  width="8" height="8" rx="2.5" fill="#AE1F22"/>
  <rect x="12" y="2"  width="8" height="8" rx="2.5" fill="#7A0A1C"/>
  <rect x="22" y="2"  width="8" height="8" rx="2.5" fill="#FFCC00"/>
  <rect x="2"  y="12" width="8" height="8" rx="2.5" fill="#D96A6A"/>
  <rect x="12" y="12" width="8" height="8" rx="2.5" fill="#AE1F22"/>
  <rect x="22" y="12" width="8" height="8" rx="2.5" fill="#7A0A1C"/>
  <rect x="2"  y="22" width="8" height="8" rx="2.5" fill="#F5CACA"/>
  <rect x="12" y="22" width="8" height="8" rx="2.5" fill="#D96A6A"/>
  <rect x="22" y="22" width="8" height="8" rx="2.5" fill="#AE1F22"/>
</svg>"""

# Shared Copy + open behaviour. Lifted from the design mock.
# `target="_top"` on Open links is set at HTML render time, not here.
_COPY_SCRIPT = """
(function(){
  function wireCopy(btn){
    if(!btn) return;
    btn.addEventListener('click', function(){
      var url = btn.getAttribute('data-url');
      function done(){
        var lab = btn.querySelector('.label');
        if(!lab) return;
        var prev = lab.textContent;
        lab.textContent = 'Copied';
        btn.classList.add('copied');
        setTimeout(function(){
          lab.textContent = prev;
          btn.classList.remove('copied');
        }, 1600);
      }
      function fallback(){
        var ta = document.createElement('textarea');
        ta.value = url; ta.style.position = 'fixed'; ta.style.opacity = '0';
        document.body.appendChild(ta); ta.select();
        try { document.execCommand('copy'); } catch(e) {}
        document.body.removeChild(ta);
        done();
      }
      if (navigator.clipboard && navigator.clipboard.writeText) {
        navigator.clipboard.writeText(url).then(done).catch(fallback);
      } else {
        fallback();
      }
    });
  }
  document.querySelectorAll('[data-copy-button]').forEach(wireCopy);
})();
"""


# ═════════════════════════════════════════════════════════════════════════════
# LOGIN WELCOME PANEL
# ═════════════════════════════════════════════════════════════════════════════

def _login_welcome_html() -> str:
    """Build the full iframe HTML for the login-screen welcome panel."""
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
{_FONT_IMPORT}
{_BRAND_TOKENS}
body{{
  font-family:"Geist",system-ui,-apple-system,"Segoe UI",sans-serif;
  color:var(--ink);
  -webkit-font-smoothing:antialiased;
  text-rendering:optimizeLegibility;
  padding:8px 4px 4px 4px;
}}
.wrap{{max-width:600px;margin:0 auto;}}
.panel{{
  position:relative;background:var(--surface);
  border:1px solid var(--line);border-radius:18px;
  padding:clamp(28px,5vw,48px);
  box-shadow:0 1px 2px rgba(26,22,19,.04),
             0 26px 64px -30px rgba(26,22,19,.20);
  overflow:hidden;
}}
.panel::before{{
  content:"";position:absolute;top:0;left:0;right:0;height:3px;
  background:linear-gradient(90deg,var(--peak) 0 54px,var(--cardinal) 54px 100%);
}}
.watermark{{
  position:absolute;top:-22px;right:-22px;
  width:184px;height:184px;opacity:.05;pointer-events:none;
}}
.eyebrow{{
  display:inline-flex;align-items:center;gap:8px;
  font-family:"Geist Mono",ui-monospace,monospace;
  font-size:11px;letter-spacing:.18em;text-transform:uppercase;
  color:var(--cardinal);font-weight:500;margin-bottom:22px;
}}
.eyebrow .dot{{width:6px;height:6px;border-radius:50%;background:var(--cardinal);}}
.lockup{{display:flex;align-items:center;gap:13px;margin-bottom:24px;}}
.lockup .word{{
  font-family:"Fraunces",Georgia,serif;font-weight:600;
  font-size:22px;letter-spacing:-.01em;color:var(--ink);line-height:1.1;
}}
.lockup .formerly{{
  font-family:"Geist Mono",ui-monospace,monospace;
  font-size:10.5px;color:var(--soft);margin-top:3px;
}}
.panel h1{{
  font-family:"Fraunces",Georgia,serif;font-weight:600;
  font-size:clamp(2rem,5.4vw,2.9rem);line-height:1.04;
  letter-spacing:-.02em;color:var(--ink);margin-bottom:18px;
}}
.msg{{max-width:50ch;}}
.msg p{{font-size:clamp(1rem,2.4vw,1.07rem);line-height:1.62;color:var(--body);}}
.msg p + p{{margin-top:12px;}}
.msg .inline-url{{
  font-family:"Geist Mono",ui-monospace,monospace;font-weight:500;color:var(--cardinal);
}}
.msg .sig{{
  margin-top:16px;font-family:"Fraunces",Georgia,serif;
  font-style:italic;font-weight:500;font-size:1.08rem;color:var(--ink);
}}
.actions{{display:flex;flex-wrap:wrap;gap:12px;align-items:center;margin-top:26px;}}
.cta{{
  display:inline-flex;align-items:center;gap:10px;
  background:var(--cardinal);color:#fff;text-decoration:none;
  font-family:"Geist",system-ui,sans-serif;font-weight:600;font-size:1rem;
  padding:14px 22px;border-radius:11px;
  transition:transform .15s ease,background .15s ease,box-shadow .15s ease;
  box-shadow:0 1px 2px rgba(122,10,28,.18),0 12px 24px -14px rgba(122,10,28,.5);
}}
.cta:hover{{background:var(--cardinal-deep);transform:translateY(-1px);}}
.cta .arrow{{transition:transform .15s ease;}}
.cta:hover .arrow{{transform:translateX(3px);}}
.copy{{
  display:inline-flex;align-items:center;gap:9px;background:#fff;color:var(--ink);
  border:1px solid var(--line);
  font-family:"Geist",system-ui,sans-serif;font-weight:600;font-size:1rem;
  padding:13px 18px;border-radius:11px;cursor:pointer;
  transition:border-color .15s ease,color .15s ease,background .15s ease,transform .15s ease;
}}
.copy:hover{{border-color:var(--cardinal);color:var(--cardinal);transform:translateY(-1px);}}
.copy.copied{{border-color:var(--cardinal);color:var(--cardinal);background:#FCF3F3;}}
.cta:focus-visible,.copy:focus-visible{{outline:3px solid rgba(194,135,15,.55);outline-offset:2px;}}
.signin{{
  margin-top:26px;display:flex;gap:12px;align-items:flex-start;
  border:1px solid var(--line);border-left:3px solid var(--gold);
  background:#FCFAF5;border-radius:10px;padding:14px 16px;
}}
.signin svg{{flex:none;margin-top:2px;color:var(--gold);}}
.signin .t{{font-size:.92rem;line-height:1.5;color:var(--ink);}}
.signin .t .mono{{font-family:"Geist Mono",ui-monospace,monospace;font-weight:500;}}
.signin .t .note{{
  display:block;color:var(--muted);margin-top:4px;font-size:.86rem;
}}
.foot{{
  margin-top:28px;padding-top:18px;border-top:1px solid var(--line-soft);
  font-family:"Geist Mono",ui-monospace,monospace;
  font-size:11px;color:var(--soft);line-height:1.65;
}}
</style>
</head>
<body>
  <div class="wrap">
    <main class="panel" role="main">
      <div class="watermark">{_LABDASH_MARK_SVG.format(w=184, h=184)}</div>
      <div class="eyebrow"><span class="dot"></span>We&rsquo;ve moved</div>
      <div class="lockup">
        {_LABDASH_MARK_SVG.format(w=42, h=42)}
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
        <a class="cta" href="{NEW_APP_URL}" target="_top">
          Open the new dashboard <span class="arrow" aria-hidden="true">&rarr;</span>
        </a>
        <button class="copy" type="button" data-copy-button data-url="{NEW_APP_URL}"
                aria-label="Copy the new address">
          <svg width="15" height="15" viewBox="0 0 24 24" fill="none" aria-hidden="true">
            <rect x="9" y="9" width="11" height="11" rx="2"
                  stroke="currentColor" stroke-width="1.7"/>
            <path d="M5 15V5a2 2 0 0 1 2-2h10"
                  stroke="currentColor" stroke-width="1.7" stroke-linecap="round"/>
          </svg>
          <span class="label">Copy address</span>
        </button>
      </div>
      <div class="signin">
        <svg width="16" height="16" viewBox="0 0 24 24" fill="none" aria-hidden="true">
          <path d="M12 3l7 3v5c0 4.2-2.9 7.6-7 8.7C7.9 18.6 5 15.2 5 11V6l7-3z"
                stroke="currentColor" stroke-width="1.6" stroke-linejoin="round"/>
        </svg>
        <div class="t">
          Sign in with your <span class="mono">@usc.edu</span> Microsoft account.
          <span class="note">
            <span class="mono">@med.usc.edu</span> accounts aren&rsquo;t supported yet.
          </span>
        </div>
      </div>
      <div class="foot">
        Please update your bookmarks. This page will retire once everyone has moved over.
      </div>
    </main>
  </div>
  <script>{_COPY_SCRIPT}</script>
</body>
</html>"""


# CSS-only markup for the "OR" divider + fallback lead-in between the
# welcome iframe and the native password form. No JS in here, so it
# can ride directly through st.markdown rather than another iframe.
_LOGIN_OR_DIVIDER_HTML = """
<style>
.labdash-or-wrap{
  max-width:480px;margin:22px auto 18px auto;padding:0 4px;
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
    """
    _components_html(_login_welcome_html(), height=820, scrolling=False)
    st.markdown(_LOGIN_OR_DIVIDER_HTML, unsafe_allow_html=True)


# ═════════════════════════════════════════════════════════════════════════════
# DASHBOARD RETIREMENT BANNER
# ═════════════════════════════════════════════════════════════════════════════

def _dashboard_banner_html() -> str:
    """Build the full iframe HTML for the retirement banner mounted at
    the top of the authenticated dashboard.
    """
    return f"""<!doctype html>
<html lang="en">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<style>
{_FONT_IMPORT}
{_BRAND_TOKENS}
body{{
  font-family:"Geist",system-ui,-apple-system,"Segoe UI",sans-serif;
  color:var(--ink);
  -webkit-font-smoothing:antialiased;
}}
.depbar{{
  display:flex;align-items:center;gap:16px;flex-wrap:wrap;
  background:linear-gradient(180deg,#FCF5E2,#FBF1D8);
  border:1px solid #ECD9A6;border-radius:10px;
  box-shadow:0 6px 18px -12px rgba(122,60,0,.30);
  padding:11px 18px;
}}
.depbar .icon{{flex:none;display:flex;}}
.depbar .txt{{
  font-size:.92rem;line-height:1.4;color:var(--ink);min-width:240px;
}}
.depbar .txt .mono{{
  font-family:"Geist Mono",ui-monospace,monospace;font-weight:500;color:var(--cardinal);
}}
.depbar .txt strong{{font-weight:600;}}
.depbar .txt .note{{
  display:block;color:#7a6a42;font-size:.78rem;margin-top:2px;
}}
.depbar .spacer{{flex:1 1 12px;}}
.depbar .row{{display:flex;align-items:center;gap:9px;}}
.bb{{
  display:inline-flex;align-items:center;gap:8px;
  font-family:"Geist",system-ui,sans-serif;font-weight:600;font-size:.9rem;
  padding:9px 15px;border-radius:9px;cursor:pointer;
  border:1px solid transparent;text-decoration:none;
  transition:transform .14s ease,background .14s ease,
             border-color .14s ease,color .14s ease,box-shadow .14s ease;
}}
.bb-primary{{
  background:var(--cardinal);color:#fff;
  box-shadow:0 1px 2px rgba(122,10,28,.18),0 10px 20px -14px rgba(122,10,28,.55);
}}
.bb-primary:hover{{background:var(--cardinal-deep);transform:translateY(-1px);}}
.bb-primary .arrow{{transition:transform .14s ease;}}
.bb-primary:hover .arrow{{transform:translateX(2px);}}
.bb-ghost{{
  background:rgba(255,255,255,.65);border-color:#E2CF9B;color:#6b4f12;
}}
.bb-ghost:hover{{
  border-color:var(--gold);color:var(--gold);background:#fff;
}}
.bb-ghost.copied{{border-color:var(--cardinal);color:var(--cardinal);}}
.bb:focus-visible{{
  outline:3px solid rgba(194,135,15,.55);outline-offset:2px;
}}
.dismiss{{
  flex:none;width:30px;height:30px;border-radius:8px;
  border:1px solid transparent;background:transparent;color:#9c8a5c;cursor:pointer;
  display:grid;place-items:center;font-size:17px;line-height:1;
  transition:background .14s ease,color .14s ease;
}}
.dismiss:hover{{background:rgba(150,110,20,.10);color:#6b4f12;}}
</style>
</head>
<body>
  <div class="depbar" id="depbar" role="region" aria-label="Service notice">
    <span class="icon">{_LABDASH_MARK_SVG.format(w=26, h=26)}</span>
    <div class="txt">
      LabDash has moved to <span class="mono">labdash.micbask.com</span>.
      This version will be retired <strong>soon</strong>, please switch over when you can.
      <span class="note">Sign in on the new site with your @usc.edu Microsoft account.</span>
    </div>
    <span class="spacer"></span>
    <div class="row">
      <a class="bb bb-primary" href="{NEW_APP_URL}" target="_top">
        Open new dashboard <span class="arrow" aria-hidden="true">&rarr;</span>
      </a>
      <button class="bb bb-ghost" type="button" data-copy-button data-url="{NEW_APP_URL}">
        <span class="label">Copy address</span>
      </button>
      <button class="dismiss" type="button" id="dismissBtn" aria-label="Dismiss notice">
        &times;
      </button>
    </div>
  </div>
  <script>
    (function(){{
      var KEY = 'labdash-banner-dismissed-v1';
      var bar = document.getElementById('depbar');
      // Hide immediately on load if previously dismissed in this
      // browser. Iframe inherits storage of its OWN origin (the
      // Streamlit component endpoint), so dismissal persists across
      // Streamlit reruns + page reloads on the same Streamlit host.
      try {{
        if (window.localStorage && window.localStorage.getItem(KEY) === '1') {{
          if (bar) bar.style.display = 'none';
        }}
      }} catch (e) {{}}
      var dismiss = document.getElementById('dismissBtn');
      if (dismiss) {{
        dismiss.addEventListener('click', function(){{
          if (bar) bar.style.display = 'none';
          try {{ window.localStorage.setItem(KEY, '1'); }} catch (e) {{}}
        }});
      }}
    }})();
    {_COPY_SCRIPT}
  </script>
</body>
</html>"""


def render_dashboard_banner() -> None:
    """Render the retirement banner at the top of the authenticated
    dashboard (called once on every rerun, on both analytics and
    pre-analytics dashboards).
    """
    _components_html(_dashboard_banner_html(), height=110, scrolling=False)
