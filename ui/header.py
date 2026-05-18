"""
ui/header.py — Dashboard banner (USC maroon strip + bench + date subtitle).

render_header is called once at the top of every analytics /
pre-analytics view. It injects scoped CSS for the banner each
call — cheap and reliable across reruns. The styling matches the
USC brand bar at the top of the page.
"""

import pandas as pd
import streamlit as st


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
      /* Banner — DESKTOP layout uses full-bleed-minus-sidebar calc()
         geometry: width = stMain (100vw - 320px sidebar), margin-left
         calc shift to pin the bar to the sidebar's right edge. On
         MOBILE (≤768 px) the sidebar is an overlay (not a 320 px
         column), so the calc() margins shift the banner LEFT 160 px
         off-screen and the width is `100vw - 320px` which on a 390 px
         phone = 70 px — producing the 1-char-wide vertical title
         column in the user's screenshot. The geometry is gated to
         desktop and a mobile counterpart resets margins to 0 and
         width to 100%.
         The unconditional properties (background, padding, position,
         align-items, gap, border-radius, box-sizing) apply at every
         viewport so the banner looks coherent on both. */
      {_banner_sel} {{
          position: relative !important;
          background: #1C1917 !important;
          padding: 20px 24px !important;
          margin-top: 0 !important;
          margin-bottom: 0 !important;
          align-items: center !important;
          gap: 16px !important;
          border-radius: 0 !important;
          box-sizing: border-box !important;
      }}
      @media (min-width: 768.01px) {{
          {_banner_sel} {{
              margin-left: calc(50% - 50vw + 160px) !important;
              margin-right: 0 !important;
              width: calc(100vw - 320px) !important;
              max-width: calc(100vw - 320px) !important;
          }}
      }}
      @media (max-width: 768px) {{
          {_banner_sel} {{
              margin-left: 0 !important;
              margin-right: 0 !important;
              width: 100% !important;
              max-width: 100% !important;
          }}
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

      /* ── Inactive nav button — transparent, muted white text. ALL
            CAPS with slight letter-spacing for typographic clarity at
            12 px size; applied to BOTH outer button + inner p/div so
            baseweb's possible inner `text-transform: none` cannot win. */
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
          text-transform: uppercase !important;
          letter-spacing: 0.04em !important;
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
          letter-spacing: 0.04em !important;
          text-transform: uppercase !important;
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
            the hover rule above so hover doesn't dim the active gold.
            text-transform + letter-spacing repeated here so the active
            rule wins specificity on those properties too (defensive). */
      html body {_pill_sel} .st-key-{_active_key} button,
      html body {_pill_sel} .st-key-{_active_key} button:hover,
      html body {_pill_sel} .st-key-{_active_key} button:focus,
      html body .st-key-{_active_key} button,
      html body .st-key-{_active_key} button:hover,
      html body .st-key-{_active_key} button:focus {{
          background: #F1AB1F !important;
          background-color: #F1AB1F !important;
          color: #1C1917 !important;
          text-transform: uppercase !important;
          letter-spacing: 0.04em !important;
      }}
      html body {_pill_sel} .st-key-{_active_key} button p,
      html body {_pill_sel} .st-key-{_active_key} button div,
      html body .st-key-{_active_key} button p,
      html body .st-key-{_active_key} button div {{
          color: #1C1917 !important;
          font-weight: 500 !important;
          text-transform: uppercase !important;
          letter-spacing: 0.04em !important;
      }}
    </style>
    """, unsafe_allow_html=True)

    cols = st.columns([0.65, 0.35], vertical_alignment="center")
    with cols[0]:
        st.markdown(
            f'<div class="app-header-title">Laboratory Productivity Dashboard</div>'
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


