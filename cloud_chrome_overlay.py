"""Cloud chrome overlay covers — geometric fix for two persistent issues:

1. Light gap above the dark banner on Streamlit Cloud (residual
   stHeader / stAppViewContainer top-offset that survived every
   `display: none` / `top: 0 !important` attempt because Streamlit's
   theme inline styles beat external !important).
2. Bottom-right Cloud overlays visible to public viewers (Streamlit
   logo / "Hosted with Streamlit" badge + deployer avatar). Class-name
   prefix matchers (`[class*="viewerBadge"]` etc.) failed because the
   current Cloud build uses hashed class names that drift per release.

Approach: paint two fixed-position pseudo-element rectangles directly
over the offending zones with the SAME colour as the surrounding UI.
The user sees the rectangles instead of the chrome — invisible because
the colour matches what should be there.

  • body::before — dark bar across the top of the main column
    (left: 320 px past sidebar, full remaining width, height 80 px,
    background #1a1a1a matching the banner). Sits at z-index 100;
    the banner itself is raised to z-index 200 so banner content
    renders ABOVE the cover.
  • body::after  — light rectangle at the bottom-right corner
    (280 × 140 px, background #f4f4f4 matching the page bg).
    z-index 2147483647 (max 32-bit int) — beats every Cloud overlay
    regardless of its own z-index.

pointer-events: none on both covers so clicks pass through to the
content beneath (or to nothing, where the Cloud chrome was).

This module is imported and called from app.py AFTER the main
inject_css() so the overlay rules layer on top of (and cascade-win
against) the global CSS.
"""

import streamlit as st


_OVERLAY_CSS = """
<style>
body::before {
    content: '' !important;
    position: fixed !important;
    top: 0 !important;
    left: 320px !important;
    right: 0 !important;
    height: 80px !important;
    background-color: #1a1a1a !important;
    z-index: 100 !important;
    pointer-events: none !important;
}
body::after {
    content: '' !important;
    position: fixed !important;
    bottom: 0 !important;
    right: 0 !important;
    width: 280px !important;
    height: 140px !important;
    background-color: #f4f4f4 !important;
    z-index: 2147483647 !important;
    pointer-events: none !important;
}
div[data-testid="stHorizontalBlock"]:has(.app-header-title) {
    position: relative !important;
    z-index: 200 !important;
}
.app-header-stripe {
    position: relative !important;
    z-index: 200 !important;
}
</style>
"""


def inject_overlay_covers() -> None:
    """Inject the geometric cover overlays. Call AFTER inject_css()."""
    st.markdown(_OVERLAY_CSS, unsafe_allow_html=True)
