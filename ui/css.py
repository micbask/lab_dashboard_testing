"""
ui/css.py — Global CSS injection + sidebar resize-kill JS shim.

The bulk of this module is a single large _GLOBAL_CSS string injected
via st.markdown at app startup. It styles every Streamlit primitive
the dashboard uses — sidebar, buttons, selectboxes, radios, sliders,
date inputs, st.metric, KPI cards, the section heading underline,
the heatmap legend chips, the data-management expander, and the
admin login overlay.

The inject_sidebar_resize_kill helper installs a JS MutationObserver
via components.html(height=0) that aggressively re-overrides the
inline `cursor: col-resize` style Streamlit 1.50+ sets on the
sidebar drag handle — pure CSS isn't enough since the styled-
components layer re-emits the inline style on rerender.
"""

import pandas as pd
import streamlit as st


# ═════════════════════════════════════════════════════════════════════════════
# CSS INJECTION
# ═════════════════════════════════════════════════════════════════════════════

def inject_css() -> None:
    """Inject all global CSS styles."""
    st.markdown(_GLOBAL_CSS, unsafe_allow_html=True)


def inject_sidebar_resize_kill() -> None:
    """Defensive JS shim that removes the sidebar's resize cursor.

    Streamlit ≥1.50 renders the sidebar via the `re-resizable` library,
    which sets `style="cursor: col-resize"` INLINE on the right-edge
    handle div and re-applies it on every rerender via styled-components.
    CSS alone (see _GLOBAL_CSS) catches the common cases, but the
    inline style can race the stylesheet on rerender. This shim
    installs a MutationObserver that aggressively overrides any
    `cursor: *-resize` inline style inside the sidebar.

    Implemented via `st.components.v1.html(height=0)` so the script
    runs once at app load. The iframe is height-zero so it's
    visually invisible. window.parent.document reaches across the
    iframe boundary (same-origin on Streamlit Cloud).
    """
    import streamlit.components.v1 as _components
    _components.html(
        """
        <script>
        (function () {
            const root = window.parent && window.parent.document;
            if (!root) return;
            const kill = () => {
                const sb = root.querySelector('[data-testid="stSidebar"]');
                if (!sb) return;
                sb.querySelectorAll('div').forEach((el) => {
                    const c = el.style && el.style.cursor;
                    if (c && c.indexOf('resize') !== -1) {
                        el.style.setProperty('cursor', 'default', 'important');
                        el.style.setProperty('pointer-events', 'none', 'important');
                        el.style.setProperty('display', 'none', 'important');
                    }
                });
            };
            kill();
            new MutationObserver(kill).observe(root.body, {
                subtree: true,
                attributes: true,
                attributeFilter: ['style', 'class'],
                childList: true,
            });
        })();
        </script>
        """,
        height=0,
    )


_GLOBAL_CSS = """
<style>
/* ═══════════════════════════════════════════════════════
   BASE — font + background
   ═══════════════════════════════════════════════════════
   Light surface (#f4f4f4) for the main content area; sidebar
   gets its own dark surface via the SIDEBAR section below.
   Inter is the dashboard's primary typeface. */
html, body,
[data-testid="stApp"], .stApp,
[data-testid="stAppViewContainer"],
[data-testid="stMain"], section.main,
[data-testid="stMainBlockContainer"], .stMainBlockContainer {
    font-family: 'Inter', system-ui, -apple-system, sans-serif !important;
    background-color: #f4f4f4 !important;
}

/* Clip horizontal overflow at the html/body level. The dark header
   banner and cardinal stripe use `width: calc(100vw - 320px)` for
   the full-bleed-minus-sidebar effect, but `100vw` includes the
   vertical-scrollbar gutter (~15-17 px on macOS Chrome / Windows),
   while the actual visible viewport area is `100vw - scrollbar_width`.
   On any page tall enough to need a vertical scrollbar, the banner
   ends up ~17 px wider than visible — combined with the
   `overflow-x: visible` rule on stMain/block-container below (which
   we need so the banner isn't clipped from the LEFT), that overflow
   propagates up to body and produces an unwanted horizontal page
   scrollbar. Clipping at html/body keeps the banner edge-to-edge
   visually while preventing the page itself from scrolling sideways.
   The 17 px of clipped area is only scrollbar-gutter air; no real
   content lives there. */
html, body {
    overflow-x: hidden !important;
}

/* ═══════════════════════════════════════════════════════
   TOP-OF-PAGE LAYOUT — eliminate the empty strip above
   the dashboard's dark banner
   ═══════════════════════════════════════════════════════
   Two source-confirmed causes stack to produce the visible
   strip on Streamlit Cloud (verified against streamlit/
   streamlit develop @ 2026-05-12; pinned to 1.57.0 in
   requirements.txt to keep these selectors valid):

   (A) `<StyledHeader data-testid="stHeader">` is rendered
       at `position: absolute; top: 0; height: 3.75rem` with
       a background that ranges from transparent to bgColor
       depending on whether it has content. With
       `client.toolbarMode = "viewer"` (.streamlit/config.toml:12)
       the header has no children — it's a 60-px-tall
       transparent overlay sitting over stMainContent.
       Source: frontend/app/src/components/Header/styled-components.ts
       Source: frontend/app/src/components/Header/Header.tsx

   (B) `<StyledFlexContainerBlock data-testid="stVerticalBlock">`
       is the main column's flex container; it uses
       `gap: theme.spacing.lg = 1rem = 16 px` between every
       direct child. Every `st.markdown(<style>/<script>)`
       call (used here for title-pin + CSS injection) creates
       an empty `<div data-testid="stElementContainer">` that
       contributes one 16-px gap slot — N injections × 16 px
       = the visible light strip above the banner.
       Source: frontend/lib/src/components/core/Block/
               styled-components.ts (StyledFlexContainerBlock
               with translateGapWidth → theme.spacing.lg)
       Source: frontend/lib/src/components/core/Block/utils.ts
               (getClassnamePrefix(Direction.VERTICAL) ===
               "stVerticalBlock")
       Source: frontend/lib/src/theme/primitives/spacing.ts
               (spacing.lg === "1rem")

   The same source file contains direct evidence the gap is
   intentional: `StyledElementContainer` short-circuits the
   `elementType === "empty"` case to `display: none` with the
   verbatim comment "Use display: none for empty elements to
   avoid the flexbox gap" — exactly the workaround we're
   replicating for our style/script-only wrappers below.

   Earlier diagnostics blamed `stMainBlockContainer.padding-top:
   6rem` and tried specificity battles to override it. That
   rule IS being applied at runtime (single-class emotion vs.
   single-class !important override, source order favours the
   override). The padding has been 0 for several commits;
   the residual gap is entirely the flex-gap × N empty slots
   pathway above. */

/* (A) Header overlay → zero footprint, DESKTOP ONLY.
   Gated to `@media (min-width: 768.01px)` so on mobile (≤768 px)
   Streamlit's native header renders — most importantly, the
   stExpandSidebarButton inside [data-testid="stHeader"] >
   [data-testid="stToolbar"] which is the ONLY way to reopen the
   sidebar once it's been closed on a phone. Verified via the 1.57.0
   frontend bundle: in `viewer` toolbarMode, MainMenu + StatusWidget
   still render in the header, so the `> *` / stToolbar display:none
   rules ARE doing real work on desktop and must be preserved (just
   gated). The boundary uses 768.01 to align with Streamlit's own
   `@media (max-width: 768px)` sidebar-overlay rule which IS
   inclusive at 768; without the 0.01 offset, iPad mini portrait
   (768 px) gets desktop CSS but Streamlit applies the overlay rule
   → conflict. */
@media (min-width: 768.01px) {
    [data-testid="stHeader"],
    header[data-testid="stHeader"],
    .stAppHeader {
        background: transparent !important;
        height: 0 !important;
        min-height: 0 !important;
    }
    /* Hide every direct child of the header — pairs with
       client.toolbarMode = "viewer" so deploy / toolbar / menu /
       status widget render to nothing even if Cloud re-enables them. */
    [data-testid="stHeader"] > *,
    [data-testid="stToolbar"],
    .stAppToolbar {
        display: none !important;
    }
}

/* (B1) Zero the flex gap on the MAIN column's TOP-LEVEL
   vertical block. The two-`>` direct-child chain is LOAD-
   BEARING — it pins the rule to exactly the page-level block
   and prevents leakage into nested `st.columns` / `st.expander`
   / `st.tabs` / `st.container` (all of which reuse the
   `stVerticalBlock` testid). Do NOT broaden these selectors. */
[data-testid="stMain"]
    > [data-testid="stMainBlockContainer"]
    > [data-testid="stVerticalBlock"] {
    gap: 0 !important;
}

/* (B2) Same intent with a descendant combinator between
   stMainBlockContainer and stVerticalBlock — defensive coverage
   if a future Streamlit release inserts a wrapper there.
   As of develop 2026-05-12 no such wrapper exists. The trailing
   `>` still pins to the immediate vertical-block child to keep
   the rule from leaking into nested columns/expanders/tabs. */
[data-testid="stMain"]
    [data-testid="stMainBlockContainer"]
    > [data-testid="stVerticalBlock"] {
    gap: 0 !important;
}

/* (B3) Belt-and-braces — remove empty <style>-only /
   <script>-only stElementContainer wrappers from layout
   entirely. The flex-gap fix above (B1/B2) already zeroes
   their contribution; this rule additionally collapses them
   to `display: none` so they cannot contribute even if the
   gap rule fails (e.g., Streamlit renames `stVerticalBlock`
   in a future release). `:has()` is supported in evergreen
   Chromium/WebKit/Firefox since late 2023; the fallback is
   B1/B2 working as expected.
   Source: frontend/lib/src/components/core/Block/
           ElementContainer.tsx renders <StyledElementContainer
           data-testid="stElementContainer">. */
[data-testid="stElementContainer"]:has(
    > [data-testid="stMarkdownContainer"] > div:only-child > style:only-child
),
[data-testid="stElementContainer"]:has(
    > [data-testid="stMarkdownContainer"] > div:only-child > script:only-child
),
[data-testid="stElementContainer"]:has(> [data-testid="stMarkdownContainer"]:empty) {
    display: none !important;
}

/* (C) stMainBlockContainer padding-top → 0 so the banner sits
   flush at the very top of the content column. This rule has
   been present for several commits and is genuinely working;
   restated here for clarity alongside its sibling rules above.
   Source: frontend/app/src/components/AppView/styled-components.ts
           StyledAppViewBlockContainer.paddingTop = "6rem"
           default (for non-embedded apps without top nav). */
.stMainBlockContainer,
[data-testid="stMainBlockContainer"] {
    padding-top: 0 !important;
}
.block-container {
    padding-top: 0 !important;
    padding-bottom: 2rem !important;
    padding-left: 24px !important;
    padding-right: 24px !important;
    max-width: 1480px !important;
}
/* On mobile (≤768 px), shrink horizontal padding so the content
   area isn't eating 12% of a 390 px viewport. 12 px each side
   leaves ~93% of viewport for the heatmap / KPI cards / banner. */
@media (max-width: 768px) {
    .block-container {
        padding-left: 12px !important;
        padding-right: 12px !important;
    }
}

/* (D) stApp / stAppViewContainer defensive padding reset —
   source confirms these are at top:0 with no padding by
   default, but cheap insurance against a future regression. */
[data-testid="stApp"],
[data-testid="stAppViewContainer"] {
    padding-top: 0 !important;
}

/* ═══════════════════════════════════════════════════════
   FULL-BLEED OVERFLOW SAFETY NET
   ═══════════════════════════════════════════════════════
   The dashboard's dark banner + cardinal stripe use
   `width: calc(100vw - 320px)` with negative-margin shifting
   (see render_header below) to bleed past block-container's
   max-width: 1480px and reach stMain's full content area.
   If any wrapper has `overflow-x: hidden` (Streamlit
   occasionally adds this on newer releases) the bar gets
   clipped. Force every plausible wrapper to allow horizontal
   overflow. */
[data-testid="stMain"],
[data-testid="stAppViewContainer"],
[data-testid="stMain"] > div,
[data-testid="stMain"] .block-container,
[data-testid="stMain"] [data-testid="stVerticalBlock"],
[data-testid="stMain"] [data-testid="stMainBlockContainer"] {
    overflow-x: visible !important;
}

/* ═══════════════════════════════════════════════════════
   SIDEBAR — dark background, light text, locked to 320 px
   ═══════════════════════════════════════════════════════
   Width is fixed so the user can't drag-resize the sidebar.
   320 px gives enough room for the section labels and the
   date input on a single line without wrapping. The resize
   handle (split into [data-testid=stSidebarResizer] in some
   Streamlit versions and a bare role="separator" in others)
   is hidden via both selectors so the right edge has no
   visible drag affordance. */
/* Sidebar — ALWAYS dark, but width-locked + in-flow only on
   desktop. On mobile (≤768 px) Streamlit's native CSS turns the
   sidebar into a position:absolute overlay; our previous ungated
   `width: 320px !important; position: relative !important;` rules
   were beating that overlay rule (via !important) and pinning the
   sidebar at 320 px of in-flow column even on a 390 px phone —
   which is THE root cause of the screenshot the user submitted.
   Gating width + position to `@media (min-width: 768.01px)` lets
   Streamlit's mobile-overlay behavior take over below the
   breakpoint. Background + border stay ungated so the dark theme
   applies regardless of viewport.
   The 768.01 boundary aligns cleanly with Streamlit's own
   `(max-width: 768px)` overlay rule (inclusive at 768) — see the
   stHeader gating comment above. */
[data-testid="stSidebar"],
section[data-testid="stSidebar"] {
    background-color: #1C1917 !important;
    border-right: 1px solid rgba(255, 255, 255, 0.08) !important;
}
@media (min-width: 768.01px) {
    [data-testid="stSidebar"],
    section[data-testid="stSidebar"] {
        width: 320px !important;
        min-width: 320px !important;
        max-width: 320px !important;
        position: relative !important;
    }
    section[data-testid="stSidebar"] > div:first-child {
        width: 320px !important;
    }
}
/* Streamlit ≥1.50 rewrote the sidebar to use the `re-resizable`
   library. The right-edge drag handle is now a child of
   [data-testid="stSidebar"] with `cursor: col-resize` set INLINE on
   the wrapper div (no data-testid, no role="separator" anymore in
   1.57.0 — confirmed by inspecting the rendered DOM). Inline styles
   beat external CSS unless we match them on the style attribute and
   use !important.
   Companion JS shim (inject_sidebar_resize_kill) installs a
   MutationObserver that re-applies these overrides on every
   styled-components rerender — needed because Emotion can race
   the stylesheet and re-emit the inline style. */
[data-testid="stSidebar"] div[style*="col-resize"],
[data-testid="stSidebar"] div[style*="col-resize"] * {
    display: none !important;
    cursor: default !important;
    pointer-events: none !important;
}
/* Fallback: any other cursor declarations on the sidebar itself
   default to arrow. Cheaper than enumerating every nested element. */
section[data-testid="stSidebar"] {
    cursor: default !important;
}
/* Small muted caption used for date ranges + hour-range readouts under
   their respective inputs. 10 px / 40 % white, no extra spacing.
   Higher specificity needed because Streamlit's
   "[data-testid=stMarkdownContainer] p" rule otherwise wins and re-
   applies 0.82rem / weight 500 / #e8e8e8 to the inner text. */
html body section[data-testid="stSidebar"] .sidebar-meta-caption,
html body section[data-testid="stSidebar"] .sidebar-meta-caption p,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] .sidebar-meta-caption,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] .sidebar-meta-caption p {
    font-size: 10px !important;
    color: rgba(255, 255, 255, 0.4) !important;
    margin-top: 6px !important;
    margin-bottom: 0 !important;
    line-height: 1.4 !important;
    font-weight: 400 !important;
    letter-spacing: 0 !important;
    text-transform: none !important;
    font-family: 'Inter', system-ui, sans-serif !important;
}
[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] p,
[data-testid="stSidebar"] label {
    color: #e8e8e8 !important;
    font-size: 0.82rem !important;
    font-weight: 500 !important;
}
/* Manual section labels. Rendered via st.markdown('<div
   class="sidebar-section-label">…</div>') from the analytics +
   pre-analytics sidebars. Manual labels sidestep Streamlit's
   high-specificity heading CSS so the subdued look stays consistent. */
html body section[data-testid="stSidebar"] .sidebar-section-label,
html body section[data-testid="stSidebar"] [data-testid="stMarkdownContainer"] .sidebar-section-label {
    font-family: 'Inter', system-ui, sans-serif !important;
    font-size: 11px !important;
    font-weight: 500 !important;
    letter-spacing: 0.08em !important;
    text-transform: uppercase !important;
    color: rgba(241, 171, 31, 0.65) !important;
    margin-top: 20px !important;
    margin-bottom: 8px !important;
    line-height: 1.4 !important;
    padding: 0 !important;
}
/* First label has no top margin so it sits flush under the sidebar
   top padding. Each label is rendered in its own stElementContainer,
   so :first-of-type on the class itself wouldn't disambiguate — key
   off the first stElementContainer of the sidebar vertical block. */
html body section[data-testid="stSidebar"] [data-testid="stVerticalBlock"]
    > [data-testid="stElementContainer"]:first-child
    .sidebar-section-label {
    margin-top: 0 !important;
}
[data-testid="stSidebar"] [data-testid="stCaptionContainer"],
[data-testid="stSidebar"] small,
[data-testid="stSidebar"] [data-testid="stCaptionContainer"] p {
    color: #999999 !important;
    font-size: 0.73rem !important;
    font-weight: 400 !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
}
[data-testid="stSidebar"] hr {
    border-color: rgba(255, 255, 255, 0.08) !important;
    margin: 0.6rem 0 !important;
}

/* ═══════════════════════════════════════════════════════
   BUTTONS — USC Maroon with improved visibility (issue #9)
   ═══════════════════════════════════════════════════════ */
html body .stButton > button,
html body [data-testid="stBaseButton-secondary"],
html body [data-testid="stBaseButton-primary"],
html body [data-testid="stBaseButton-primaryFormSubmit"],
html body button[kind="primary"],
html body button[kind="secondary"],
html body button[kind="tertiary"] {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border: 1px solid #57121f !important;
    border-radius: 5px !important;
    font-weight: 600 !important;
    font-size: 0.85rem !important;
    padding: 0.4rem 1rem !important;
    box-shadow: none !important;
    transition: background-color 0.15s ease !important;
    text-shadow: 0 1px 1px rgba(0,0,0,0.2) !important;
}
html body .stButton > button:hover,
html body [data-testid="stBaseButton-secondary"]:hover,
html body [data-testid="stBaseButton-primary"]:hover,
html body [data-testid="stBaseButton-primaryFormSubmit"]:hover,
html body button[kind="primary"]:hover,
html body button[kind="secondary"]:hover {
    background-color: #57121f !important;
    color: #ffffff !important;
    border-color: #4a0f1b !important;
}
html body .stButton > button:disabled,
html body [data-testid="stBaseButton-secondary"]:disabled,
html body [data-testid="stBaseButton-primary"]:disabled {
    background-color: #b89099 !important;
    color: #ffffff !important;
    border-color: #b89099 !important;
    opacity: 0.65 !important;
    cursor: not-allowed !important;
}
/* Sidebar buttons — slightly lighter for contrast on dark bg */
[data-testid="stSidebar"] html body .stButton > button,
[data-testid="stSidebar"] .stButton > button {
    background-color: #8a2035 !important;
    border-color: #6F1828 !important;
    text-shadow: 0 1px 2px rgba(0,0,0,0.3) !important;
}
[data-testid="stSidebar"] .stButton > button:hover {
    background-color: #6F1828 !important;
}

/* ═══════════════════════════════════════════════════════
   SELECTBOXES / DROPDOWNS
   ═══════════════════════════════════════════════════════ */
/* Default selectbox trigger (used in the main panel: TAT proc filter
   pop-up, admin date-range editor inside the Data Management
   expander). Maroon fill is preserved here because these triggers
   sit on the WHITE main-area background; sidebar selectboxes
   override this to a subtle dark fill below. */
[data-testid="stSelectbox"] > div > div,
[data-testid="stSelectbox"] [role="combobox"],
[data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    border: 1px solid #57121f !important;
    border-radius: 5px !important;
}
[data-testid="stSelectbox"] > div > div:focus-within,
[data-testid="stSelectbox"] > div > div[aria-expanded="true"] {
    background-color: #57121f !important;
    border-color: #4a0f1b !important;
}
[data-testid="stSelectbox"] > div > div > div,
[data-testid="stSelectbox"] [role="combobox"] > div {
    color: #ffffff !important;
}
[data-testid="stSelectbox"] svg {
    fill: #ffffff !important;
    color: #ffffff !important;
}

/* Sidebar selectbox trigger — subtle dark fill on dark sidebar. Used
   by the Monthly month picker on both analytics + pre-analytics.
   Background is a SOLID dark color (not an `rgba(...)` overlay).
   When the theme's `secondaryBackgroundColor` was dark, the previous
   `rgba(255, 255, 255, 0.05)` overlay rendered as ~#232323 (sidebar
   bg showing through 5 % white). After that theme value was reverted
   to the Streamlit-default light `#f0f2f6` to fix the calendar
   header, baseweb's Select control started using the new LIGHT color
   for its internal trigger background — the transparent overlay
   exposed it, leaving white text invisible on a near-white surface.
   A solid `#262626` keeps the trigger dark independent of any theme
   value, so the white text stays readable regardless of future
   theme changes. */
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] > div > div,
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [role="combobox"],
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child {
    background: #262626 !important;
    background-color: #262626 !important;
    color: #ffffff !important;
    border: 1px solid rgba(255, 255, 255, 0.15) !important;
    border-radius: 6px !important;
    /* No `padding` declaration here — baseweb's StyledControlContainer
       has `box-sizing: border-box; overflow: hidden; height:
       theme.sizes.minElementHeight (= 2.5rem = 40 px)` per
       uber/baseweb/src/select/styled-components.ts. Adding an outer
       `padding: 8px 12px` here previously took 16 px off the inner
       height, the nested StyledValueContainer's own `spacing.sm` (8 px
       top + 8 px bottom) consumed the rest, and the visible value text
       (~14 px line-height) got clipped to near-zero — visually empty
       trigger with a chevron. The dropdown panel renders in a separate
       baseweb popover and was unaffected, which matched the user's
       report. Removing the rule restores baseweb's default trigger
       sizing; horizontal breathing room is added below via a
       ValueContainer-level padding rule that doesn't touch height. */
}
/* Tiny horizontal padding on the inner StyledValueContainer so the
   text doesn't sit flush against the left edge of the dark trigger.
   ValueContainer-level (not ControlContainer-level) so it doesn't
   subtract from `minElementHeight` and cannot reintroduce the
   text-clipping regression above. */
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"]
    [data-baseweb="select"] > div:first-child > div:first-child {
    padding-left: 4px !important;
    padding-right: 4px !important;
}
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child:focus-within,
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] [data-baseweb="select"] > div:first-child[aria-expanded="true"] {
    background: #262626 !important;
    background-color: #262626 !important;
    border-color: #F1AB1F !important;
}
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"] svg {
    fill: rgba(255, 255, 255, 0.7) !important;
    color: rgba(255, 255, 255, 0.7) !important;
}

/* Sidebar Month picker — collapse the type-to-search <input> while
   the dropdown is CLOSED.

   baseui 12.2.0 (the Streamlit-pinned version) renders an
   `<AutosizeInput role="combobox">` real <input> inside every
   single-select trigger because `searchable` defaults to true
   (source: uber/baseweb v12.2.0 src/select/default-props.ts:42
   and src/select/select-component.tsx:510-535).
   The input sits inside the StyledValueContainer as a sibling
   AFTER the SingleValue <div> that holds the displayed value
   text. With an empty value, baseui's AutosizeInput sizer
   measures the content to scrollWidth + 2 px ≈ 2-7 px wide
   (src/select/autosize-input.tsx:23,43). On the dark sidebar
   fill (#262626), that thin 2-7 px column reads as a stray
   vertical line right after the displayed value — and any
   focus-related paint (caret, focus outline, browser-native
   focus ring) lands inside that tiny visible box.

   The previous attempt set `caret-color: transparent` on the
   input but that only hides the blinking caret — it leaves
   the input's box itself paintable. Streamlit also sets
   `caretColor: theme.colors.bodyText` via overrides on the
   same input (Selectbox.tsx:223-231), which competes for the
   cascade. Setting the input's WIDTH to 0 while the dropdown
   is closed eliminates the visible box entirely.

   The scope is restricted to `input[aria-expanded="false"]` so
   when the user clicks the chevron and the dropdown opens
   (aria-expanded flips to "true" on the input — verified per
   select-component.tsx render path), this rule no longer
   applies and the input is full-width for type-to-search.
   The displayed value remains visible because it lives in a
   separate SingleValue <div>, NOT inside this <input>.

   Sidebar-scoped only — the TAT procedure-filter multiselect
   uses `stMultiSelect` (different testid) and main-panel
   selectboxes (admin date-range editor lives in `stDateInput`,
   not `stSelectbox`) are unaffected. */
html body section[data-testid="stSidebar"] [data-testid="stSelectbox"]
    input[aria-expanded="false"] {
    width: 0 !important;
    min-width: 0 !important;
    max-width: 0 !important;
    padding: 0 !important;
    border: 0 !important;
    outline: 0 !important;
    box-shadow: none !important;
    opacity: 0 !important;
    caret-color: transparent !important;
}

/* NOTE: previously we styled [data-baseweb="popover"] / [role="listbox"]
   / [role="option"] to dark-theme the selectbox dropdown panel. That
   global rule also matched the month / year selectors inside the
   st.date_input calendar popup, which painted the calendar header
   bar dark + the header text dark = an invisible "May 2026" label.
   The rule has been removed; selectbox dropdowns and the calendar
   popup now use Streamlit's default light styling.  The sidebar
   selectbox TRIGGER above keeps its dark fill — only the open-state
   dropdown panel reverts to default. */

/* ═══════════════════════════════════════════════════════
   DATE INPUT
   ═══════════════════════════════════════════════════════ */
/* Sidebar date inputs get a much subtler treatment — no maroon fill,
   subtle outline, gold focus ring. The value text MUST be readable
   white (#ffffff) — the global `[data-testid=stTextInput] input`
   rule and baseweb's own colour rules otherwise dim the digits. */
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] input,
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"],
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"] > div,
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"] input {
    background: rgba(255, 255, 255, 0.05) !important;
    background-color: rgba(255, 255, 255, 0.05) !important;
    color: #ffffff !important;
    -webkit-text-fill-color: #ffffff !important;
    caret-color: #ffffff !important;
    border: 1px solid rgba(255, 255, 255, 0.15) !important;
    border-radius: 6px !important;
    padding: 8px 12px !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    font-family: 'Inter', system-ui, sans-serif !important;
}
/* Inner baseweb wrappers (which carry their own bg / border) need to
   be transparent so the input's own bg+border above is what shows. */
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"],
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] [data-baseweb="input"] > div {
    background: transparent !important;
    background-color: transparent !important;
    border: none !important;
    padding: 0 !important;
}
/* Calendar icon inside the input — render light so it's visible on the
   dark sidebar fill. */
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] svg {
    fill: rgba(255, 255, 255, 0.7) !important;
    color: rgba(255, 255, 255, 0.7) !important;
}
html body section[data-testid="stSidebar"] [data-testid="stDateInput"] input:focus {
    border-color: #F1AB1F !important;
    outline: none !important;
    box-shadow: 0 0 0 1px rgba(241, 171, 31, 0.35) !important;
}

/* ═══════════════════════════════════════════════════════
   DATE PICKER POPUP
   ═══════════════════════════════════════════════════════
   No custom CSS — the previous dark-theme override broke empty
   placeholder cells (rendered as solid white blocks), hover state
   (white circle clashing with the maroon selected day), and the
   month / year select. Streamlit's default light calendar is clean
   and professional; a light popup against the dark trigger input is
   an acceptable trade-off vs. a half-broken dark theme.
   ═══════════════════════════════════════════════════════ */

/* ═══════════════════════════════════════════════════════
   SIDEBAR SLIDER  — hide the redundant 0/23 min/max tick-bar
   labels above the track so the only readout is the
   .sidebar-meta-caption beneath ("12:00 AM → 11:00 PM").
   The selected-value bubble on the thumb is kept (it appears
   only on hover/drag); only the fixed min/max endpoint labels
   are hidden.
   ═══════════════════════════════════════════════════════ */
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stSliderTickBar"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stSliderTickBarMin"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stSliderTickBarMax"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stTickBar"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stTickBarMin"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stTickBarMax"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] div[class*="StyledTickBar"],
html body section[data-testid="stSidebar"] [data-testid="stSlider"] div[class*="StyledTickBarItem"] {
    display: none !important;
    visibility: hidden !important;
    height: 0 !important;
    margin: 0 !important;
    padding: 0 !important;
}

/* SIDEBAR SLIDER — recolor active track + thumbs to USC gold
   (#F1AB1F). The inactive portion of the track stays gray
   (baseweb default). Two targets:
     1. role="slider"  — the draggable thumb circles. Always
        targetable by ARIA role across baseweb versions.
     2. > div > div > div > div  — the well-known "active fill"
        depth used in every Streamlit slider color-override
        snippet in the wild. Streamlit nests the active highlight
        bar four divs deep inside [data-testid="stSlider"]. */
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [role="slider"] {
    background: #F1AB1F !important;
    background-color: #F1AB1F !important;
    border-color: #F1AB1F !important;
    box-shadow: 0 0 0 1px #F1AB1F !important;
}
html body section[data-testid="stSidebar"] [data-testid="stSlider"] > div > div > div > div {
    background: #F1AB1F !important;
    background-color: #F1AB1F !important;
}
/* Thumb-value bubble (appears on hover/drag) — recolor its text
   to gold so the floating "12" / "23" label matches the track. */
html body section[data-testid="stSidebar"] [data-testid="stSlider"] [data-testid="stThumbValue"] {
    color: #F1AB1F !important;
}

/* ═══════════════════════════════════════════════════════
   PREV / NEXT DATE NAV BUTTONS
   ═══════════════════════════════════════════════════════
   Arrow-only icon buttons (← and →) that fill their
   parent st.column. Each column is half-sidebar-width so
   on the locked 320 px sidebar the buttons are roughly
   ~140 px wide and visually balanced; the column gap
   ("medium" = 16 px) is the spacing between them.
   Transparent fill + subtle white outline; functionality
   stays identical. */

/* 16 px breathing room between the date-range caption above and
   the Prev/Next button row. The :has() selector targets the row
   that contains either nav button, so the gap appears regardless
   of which dashboard renders the row. */
html body section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(.st-key-nav_prev_date),
html body section[data-testid="stSidebar"] [data-testid="stHorizontalBlock"]:has(.st-key-nav_next_date) {
    margin-top: 16px !important;
}
html body [data-testid="stSidebar"] .st-key-nav_prev_date button,
html body [data-testid="stSidebar"] .st-key-nav_next_date button,
html body [data-testid="stSidebar"] .st-key-nav_prev_date .stButton > button,
html body [data-testid="stSidebar"] .st-key-nav_next_date .stButton > button {
    background: transparent !important;
    background-color: transparent !important;
    color: rgba(255, 255, 255, 0.7) !important;
    border: 1px solid rgba(255, 255, 255, 0.12) !important;
    font-size: 16px !important;
    font-weight: 500 !important;
    padding: 6px 0 !important;
    border-radius: 6px !important;
    text-align: center !important;
    line-height: 1 !important;
    box-shadow: none !important;
    text-shadow: none !important;
    min-height: 0 !important;
}
/* The inner <p> Streamlit wraps the button label in inherits font-
   size from its own emotion class — re-apply 16px there too so the
   arrow glyph is the right size. */
html body [data-testid="stSidebar"] .st-key-nav_prev_date button p,
html body [data-testid="stSidebar"] .st-key-nav_next_date button p,
html body [data-testid="stSidebar"] .st-key-nav_prev_date button div,
html body [data-testid="stSidebar"] .st-key-nav_next_date button div {
    font-size: 16px !important;
    font-weight: 500 !important;
    color: inherit !important;
    line-height: 1 !important;
    margin: 0 !important;
    padding: 0 !important;
}
html body [data-testid="stSidebar"] .st-key-nav_prev_date button:hover,
html body [data-testid="stSidebar"] .st-key-nav_next_date button:hover {
    background: rgba(255, 255, 255, 0.05) !important;
    background-color: rgba(255, 255, 255, 0.05) !important;
    color: #ffffff !important;
    border-color: rgba(255, 255, 255, 0.2) !important;
}
html body [data-testid="stSidebar"] .st-key-nav_prev_date button:disabled,
html body [data-testid="stSidebar"] .st-key-nav_next_date button:disabled {
    background: transparent !important;
    background-color: transparent !important;
    border-color: rgba(255, 255, 255, 0.06) !important;
    color: rgba(255, 255, 255, 0.25) !important;
    opacity: 0.6 !important;
}

/* ═══════════════════════════════════════════════════════
   RADIO BUTTONS
   ═══════════════════════════════════════════════════════ */
[data-testid="stRadio"] label {
    background-color: transparent !important;
    box-shadow: none !important;
    border: none !important;
}
[data-testid="stRadio"] label p {
    color: #e8e8e8 !important;
    font-weight: 500 !important;
    font-size: 0.88rem !important;
    background-color: transparent !important;
    text-transform: none !important;
    letter-spacing: 0 !important;
}
[data-testid="stRadio"] label:hover {
    background-color: transparent !important;
}

/* ═══════════════════════════════════════════════════════
   SIDEBAR RADIO BUTTONS → TEXT TAB STYLE
   ═══════════════════════════════════════════════════════
   Replace the filled-circle radio visual with a clean
   underlined-text-tab look: unselected options render in
   muted white, the selected option gets gold text + a 2 px
   gold underline. Underlying widget (st.radio) is unchanged
   so all selection logic / session state keeps working.
   :has(input:checked) handles the selected-state detection
   without any JS — supported in modern browsers (Chrome
   105+, Firefox 121+, Safari 15.4+), which covers the
   Streamlit Cloud target.
   Only applies inside the sidebar; main-area radios (if any
   ever appear) keep their default styling. */

/* Hide the baseweb radio circle indicator. Streamlit wraps
   each option's <input> in a div that holds the visual dot
   — that's the first <div> child of the <label>. The label's
   text is the second <div> child, which stays visible. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label > div:first-child {
    display: none !important;
}

/* Horizontal tab row — st.radio(horizontal=True) already does
   this, but we force display:flex / gap explicitly so vertical
   radio groups also pick up the tab look, and the spec'd 24 px
   gap is exact regardless of Streamlit's emotion-cache defaults. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] {
    display: flex !important;
    flex-direction: row !important;
    flex-wrap: wrap !important;
    gap: 24px !important;
    align-items: flex-end !important;
}

/* Each <label> becomes the tab. Padding gives the underline
   somewhere to sit below the text without crowding it. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label {
    color: rgba(255, 255, 255, 0.5) !important;
    font-weight: 500 !important;
    font-size: 14px !important;
    padding: 4px 0 8px 0 !important;
    border-bottom: 2px solid transparent !important;
    cursor: pointer !important;
    margin: 0 !important;
    background: transparent !important;
    background-color: transparent !important;
    transition: color 0.15s ease, border-color 0.15s ease !important;
}

/* Inner text wrapper — Streamlit nests the option label in a
   <div> (sometimes containing a <p>). Force both to inherit
   the label's colour/weight so the tab gold/white flips on the
   text itself, not just the bottom border. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label > div,
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label p,
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label span {
    color: inherit !important;
    font-weight: inherit !important;
    font-size: inherit !important;
    margin: 0 !important;
    padding: 0 !important;
    background: transparent !important;
    background-color: transparent !important;
}

/* Hover (unselected only — the :not() guard keeps the selected
   tab's gold from dimming on hover). */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label:hover:not(:has(input:checked)) {
    color: rgba(255, 255, 255, 0.8) !important;
    border-bottom-color: transparent !important;
}

/* Selected tab — gold text, gold 2 px underline. */
html body section[data-testid="stSidebar"] [data-testid="stRadio"]
    div[role="radiogroup"] label:has(input:checked) {
    color: #F1AB1F !important;
    border-bottom-color: #F1AB1F !important;
    font-weight: 500 !important;
}

/* ═══════════════════════════════════════════════════════
   TEXT INPUTS — wrapper-styled so password eye sits INSIDE
   ═══════════════════════════════════════════════════════
   Streamlit renders st.text_input(type="password") through
   baseweb's Input component. The CORRECT DOM (verified against
   uber/baseweb v13's src/input/input.tsx + base-input.tsx):

     <div data-testid="stTextInput">
       <label data-testid="stWidgetLabel">...</label>
       <div data-baseweb="input">                  ← Root WRAPPER
                                                     (gets border + bg)
         <div data-baseweb="base-input">           ← InputContainer
                                                     (has its OWN
                                                      default bg via
                                                      getInputContainer
                                                      Colors → mono200
                                                      light-gray)
           <input type="password" />
           <button>[eye SVG]</button>              ← StyledMaskTogglе
                                                     Button — SIBLING
                                                     of <input>, INSIDE
                                                     base-input
         </div>
       </div>
     </div>

   IMPORTANT — earlier comments in this file (and the now-removed
   `[data-baseweb="input"] > *:not([data-baseweb="base-input"]):not(input)`
   defensive rule) assumed the eye toggle was a DIRECT CHILD of
   `data-baseweb="input"`. It is NOT — it's two levels deep, inside
   base-input. The `> *:not(...)` direct-child rule therefore matched
   nothing in the real DOM, and the eye button had been wearing
   baseweb defaults (including ~12 px paddingRight via scale300) the
   whole time. That paddingRight + the InputContainer's own light bg
   produced the visible "white strip to the right of the eye icon"
   the user reported.

   The fix has three layers:
     (1) Root wrapper [data-baseweb="input"]: white bg + 1px border
         (unchanged from before — this is the single visible chrome).
     (2) InputContainer [data-baseweb="base-input"]: transparent bg
         (so the wrapper's white is the only visible surface) and
         zero padding (so no edge strip).
     (3) StyledMaskToggleButton (any <button> inside the wrapper):
         transparent bg, no border/shadow/outline, minimal padding
         (0 8px 0 4px) so the SVG sits close to the wrapper's right
         border with just a small visual gap.
   The <input>'s `padding-right` is trimmed from 36px → 32px since
   the eye button no longer occupies 36px of horizontal real estate.

   Scoped to [data-testid="stTextInput"] only — date inputs use
   [data-testid="stDateInput"] and keep their own existing styling. */

/* Wrapper gets the border + background. */
[data-testid="stTextInput"] [data-baseweb="input"],
[data-testid="stSidebar"] [data-testid="stTextInput"] [data-baseweb="input"] {
    background-color: #ffffff !important;
    border: 1px solid #cccccc !important;
    border-radius: 5px !important;
    padding: 0 !important;
    transition: border-color 0.15s ease, box-shadow 0.15s ease !important;
}
/* Focus ring on the wrapper when ANY descendant (input or eye) is focused. */
[data-testid="stTextInput"] [data-baseweb="input"]:focus-within,
[data-testid="stSidebar"] [data-testid="stTextInput"] [data-baseweb="input"]:focus-within {
    border-color: #6F1828 !important;
    box-shadow: 0 0 0 2px rgba(111,24,40,0.15) !important;
}
/* InputContainer (data-baseweb="base-input") — transparent so the
   wrapper's white is the only visible surface, and zero padding
   so the input + button fill the wrapper edge-to-edge. */
[data-testid="stTextInput"] [data-baseweb="input"] [data-baseweb="base-input"],
[data-testid="stSidebar"] [data-testid="stTextInput"]
    [data-baseweb="input"] [data-baseweb="base-input"] {
    background: transparent !important;
    background-color: transparent !important;
    padding: 0 !important;
    border: none !important;
}
/* The <input> element — transparent bg, no border (chrome lives on
   the wrapper). padding-right: 32px keeps the caret clear of the
   eye toggle (now flush-right with only ~12px of right-side gap). */
[data-testid="stTextInput"] [data-baseweb="input"] input,
[data-testid="stSidebar"] [data-testid="stTextInput"] [data-baseweb="input"] input {
    background-color: transparent !important;
    color: #111111 !important;
    border: none !important;
    box-shadow: none !important;
    outline: none !important;
    font-size: 0.9rem !important;
    padding-right: 32px !important;
}
/* StyledMaskToggleButton (the eye-toggle <button>) and any other
   <button> baseweb places inside the Root (e.g. clearable's clear
   button if ever used). Strip ALL default chrome and trim the
   default scale300 paddingRight so the SVG sits ~8px from the
   wrapper's right border. */
[data-testid="stTextInput"] [data-baseweb="input"] button,
[data-testid="stSidebar"] [data-testid="stTextInput"]
    [data-baseweb="input"] button {
    background: transparent !important;
    background-color: transparent !important;
    border: none !important;
    box-shadow: none !important;
    outline: none !important;
    padding: 0 8px 0 4px !important;
    margin: 0 !important;
}
/* The SVG inside the toggle button. */
[data-testid="stTextInput"] [data-baseweb="input"] button svg,
[data-testid="stSidebar"] [data-testid="stTextInput"]
    [data-baseweb="input"] button svg {
    margin: 0 !important;
    padding: 0 !important;
    display: block !important;
}

/* ═══════════════════════════════════════════════════════
   METRIC CARDS (st.metric — used in admin tools)
   ═══════════════════════════════════════════════════════ */
[data-testid="metric-container"] {
    background: #ffffff !important;
    border: 0.5px solid rgba(0, 0, 0, 0.08) !important;
    border-radius: 8px !important;
    padding: 14px !important;
    box-shadow: none !important;
}
[data-testid="metric-container"] label {
    font-size: 11px !important;
    font-weight: 500 !important;
    letter-spacing: 0 !important;
    text-transform: none !important;
    color: rgba(0, 0, 0, 0.55) !important;
}
[data-testid="metric-container"] [data-testid="stMetricValue"] {
    font-size: 22px !important;
    font-weight: 500 !important;
    color: #1a1a1a !important;
}

/* ═══════════════════════════════════════════════════════
   KPI CARD ROW (custom .metric-card HTML emitted by
   `metric_card()` in ui_components — used by every dashboard
   to render the 4–5 KPI cards above each heatmap)
   ═══════════════════════════════════════════════════════
   All five cards in a row must be equal-height. Streamlit's
   st.columns layout doesn't equalize child heights by
   default — the :has() block below stretches the column +
   inner stVerticalBlock so the .metric-card inside each
   column fills 100 % of the tallest sibling's height. The
   long-value variant (auto-applied by metric_card when the
   value is a procedure name) shrinks just the .value font
   so it can wrap without making the card taller than its
   short-value siblings. */
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card) {
    align-items: stretch !important;
    gap: 8px !important;
}
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card)
    > [data-testid="stColumn"] {
    display: flex !important;
    flex-direction: column !important;
}
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card)
    > [data-testid="stColumn"] > [data-testid="stVerticalBlock"] {
    flex: 1 !important;
    width: 100% !important;
    height: 100% !important;
    display: flex !important;
    flex-direction: column !important;
}
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card)
    > [data-testid="stColumn"] > [data-testid="stVerticalBlock"]
    > [data-testid="stElementContainer"] {
    flex: 1 !important;
    height: 100% !important;
}
[data-testid="stHorizontalBlock"]:has(> [data-testid="stColumn"] .metric-card)
    > [data-testid="stColumn"] [data-testid="stMarkdownContainer"] {
    flex: 1 !important;
    width: 100% !important;
    height: 100% !important;
}

/* ═══════════════════════════════════════════════════════
   EXPANDERS
   ═══════════════════════════════════════════════════════ */
[data-testid="stExpander"] {
    border: 1px solid #e2e8f0 !important;
    border-radius: 8px !important;
    background: #ffffff !important;
    box-shadow: 0 1px 2px rgba(0,0,0,0.04) !important;
}
[data-testid="stExpander"] summary {
    font-weight: 600 !important;
    color: #0f172a !important;
    font-size: 0.88rem !important;
}

/* ═══════════════════════════════════════════════════════
   SIDEBAR EXPANDERS — dark surface matching the sidebar
   ═══════════════════════════════════════════════════════
   Verified against streamlit/streamlit @ 1.57.0 source:
     - Outer wrapper:    [data-testid="stExpander"]
     - Native <details><summary> (1.57.0 still uses these,
       per frontend/lib/src/components/elements/Expander/
       Expander.tsx)
     - Expanded body:    [data-testid="stExpanderDetails"]
     - Chevron:          Material-Symbols font glyph inside a
                         <span data-testid="stIconMaterial"> —
                         NOT an SVG (verified against
                         MaterialFontIcon.tsx + Material/
                         styled-components.ts on 1.57.0). The
                         color is applied via the `color`
                         property, not `fill`.
   Override scope is `section[data-testid="stSidebar"]` so
   main-area expanders keep their light treatment. The global
   `[data-testid="stExpander"] { background: #ffffff }` rule
   above continues to apply to any remaining main-area expanders
   (e.g. the Data Management expander) via specificity cascade. */

/* Outer wrapper — match the sidebar surface, subtle definition. */
section[data-testid="stSidebar"] [data-testid="stExpander"] {
    background: #1C1917 !important;
    background-color: #1C1917 !important;
    border: 0.5px solid rgba(255, 255, 255, 0.08) !important;
    border-radius: 6px !important;
    box-shadow: none !important;
}

/* Expanded body — same dark surface, no inner border. */
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stExpanderDetails"] {
    background: #1C1917 !important;
    background-color: #1C1917 !important;
    border: none !important;
}

/* Header summary button — white text, slight hover lift. */
section[data-testid="stSidebar"] [data-testid="stExpander"] summary {
    color: #ffffff !important;
    font-weight: 500 !important;
    font-size: 0.88rem !important;
    background: transparent !important;
    background-color: transparent !important;
    transition: background-color 0.15s ease !important;
}
section[data-testid="stSidebar"] [data-testid="stExpander"] summary:hover {
    background: rgba(255, 255, 255, 0.03) !important;
    background-color: rgba(255, 255, 255, 0.03) !important;
}

/* Summary label text (inside the markdown <p>). */
section[data-testid="stSidebar"] [data-testid="stExpander"] summary p,
section[data-testid="stSidebar"] [data-testid="stExpander"] summary span,
section[data-testid="stSidebar"] [data-testid="stExpander"] summary [data-testid="stMarkdownContainer"] p {
    color: #ffffff !important;
}

/* Chevron — Material-Symbols font glyph in a <span data-testid=
   "stIconMaterial">. Color is what tints the glyph; `fill:` would
   be a no-op since there's no SVG. Slightly muted white so the
   chevron reads as secondary to the label text. */
section[data-testid="stSidebar"] [data-testid="stExpander"] summary [data-testid="stIconMaterial"] {
    color: rgba(255, 255, 255, 0.7) !important;
}

/* Body text inside the expander — flip ALL the previous DARK
   rules to light. The expander now matches the sidebar surface
   so widget labels, paragraph text, and inline strong/em read
   as light-on-dark. */
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] p,
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] li,
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] span,
section[data-testid="stSidebar"] [data-testid="stExpander"] label,
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stWidgetLabel"],
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stWidgetLabel"] p,
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stMarkdownContainer"] strong {
    color: #e8e8e8 !important;
}

/* Captions inside expander (st.caption) — slightly dimmer. */
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stCaptionContainer"],
section[data-testid="stSidebar"] [data-testid="stExpander"] [data-testid="stCaptionContainer"] p,
section[data-testid="stSidebar"] [data-testid="stExpander"] small {
    color: rgba(255, 255, 255, 0.55) !important;
}

/* File uploader dropzone — apply to EVERY sidebar dropzone, not
   just ones inside expanders. The analytics dashboard has a
   second uploader OUTSIDE the Data Management expander (in the
   `else` branch when GitHub storage isn't configured); without
   this broader rule it would render as a default light dropzone
   against the dark sidebar. */
section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] {
    background: rgba(255, 255, 255, 0.03) !important;
    background-color: rgba(255, 255, 255, 0.03) !important;
    border: 1px dashed rgba(255, 255, 255, 0.18) !important;
}
section[data-testid="stSidebar"] [data-testid="stFileUploaderDropzone"] * {
    color: #e8e8e8 !important;
}

/* ═══════════════════════════════════════════════════════
   DIVIDERS
   ═══════════════════════════════════════════════════════ */
hr {
    border-color: #e2e8f0 !important;
    margin: 1.1rem 0 !important;
}

/* ═══════════════════════════════════════════════════════
   CUSTOM COMPONENT CLASSES
   ═══════════════════════════════════════════════════════ */
.keck-badge {
    background: #EDC153;
    color: #3a1a00;
    font-size: 0.68rem;
    font-weight: 700;
    padding: 3px 10px;
    border-radius: 12px;
    letter-spacing: 1px;
    text-transform: uppercase;
}
.keck-date-label {
    color: #d0d8e4;
    font-size: 0.78rem;
    margin-top: 6px;
    display: block;
}
.metric-card {
    background: #ffffff;
    border: 0.5px solid rgba(0, 0, 0, 0.08);
    border-radius: 8px;
    padding: 14px;
    margin-bottom: 0;
    /* Equal-height enforcement — `min-height: 110 px` is the floor for
       every card in a row regardless of content. Combined with the
       :has() flex-stretch on the parent stHorizontalBlock (defined
       further up), short-value cards (numbers, "9 AM") match the
       tallest sibling — typically the Top procedure card, whose
       value can wrap to 2 lines via .metric-card-long below. */
    min-height: 110px;
    height: 100%;
    box-sizing: border-box;
    box-shadow: none;
    display: flex;
    flex-direction: column;
}
.metric-card .label {
    color: rgba(0, 0, 0, 0.55);
    font-size: 11px;
    font-weight: 500;
    text-transform: none;
    letter-spacing: 0;
    margin-bottom: 0;
}
.metric-card .value {
    color: #1a1a1a;
    font-size: 22px;
    font-weight: 500;
    line-height: 1;
    margin-top: 10px;
    word-break: break-word;
}
/* Long values (procedure names) — 13 px / line-height 1.3 lets the
   full name wrap to 2 lines without truncation. The equal-height row
   (align-items: stretch on the parent stHorizontalBlock) keeps cards
   in the same row at the same height regardless of which has a
   wrapped value. -webkit-line-clamp:2 is a safety net for pathological
   60+ char procedure names so they cannot push the row taller than 2
   lines of body text. word-break: break-word (inherited from .value)
   handles names without natural break points. */
.metric-card.metric-card-long .value {
    font-size: 13px;
    line-height: 1.3;
    display: -webkit-box;
    -webkit-line-clamp: 2;
    -webkit-box-orient: vertical;
    overflow: hidden;
}
.metric-card .sub {
    color: rgba(0, 0, 0, 0.45);
    font-size: 11px;
    margin-top: 8px;
}
.section-heading {
    color: #1a1a1a;
    font-size: 16px;
    font-weight: 500;
    /* Gold underline restored — subtle 1 px / 50 % USC gold so it
       reads as a quiet rhythm element rather than a hard rule.
       padding-bottom = 8 px so the line sits 8 px below the heading
       text; margin-bottom = 16 px so the line is 16 px above the
       legend; margin-top = 12 px combines with the metrics-divider's
       12 px margin above to reach the spec'd 24 px gap from the KPI
       cards row. */
    border-bottom: 1px solid rgba(241, 171, 31, 0.5);
    padding: 0 0 8px 0;
    letter-spacing: 0;
    text-transform: none;
    margin: 12px 0 16px 0;
}
.heatmap-legend {
    background: rgba(0, 0, 0, 0.02);
    border: 0.5px solid rgba(0, 0, 0, 0.06);
    border-left: 0.5px solid rgba(0, 0, 0, 0.06);
    border-radius: 6px;
    padding: 8px 12px;
    font-size: 12px;
    color: rgba(0, 0, 0, 0.6);
    margin-top: 0;
    margin-bottom: 12px;  /* 12 px gap to the heatmap below */
}

/* ═══════════════════════════════════════════════════════
   INLINE TOP-N selector (Analytics) — st.button-based
   ═══════════════════════════════════════════════════════
   The selector is rendered as three `st.button` widgets inside
   a 5-column `st.columns([6, 0.7, 0.3, 0.3, 0.3])` row alongside
   the legend prose + "Showing top" label. Clicking a button
   triggers a normal Streamlit script rerun (preserves
   session_state including app_authenticated) — NOT a browser
   navigation, which the prior `<a href="?top_n=N">` approach
   triggered and which wiped the auth session, bouncing users
   back to the login screen on every click.

   The currently-selected button is rendered with
   `type="primary"` (others `type="secondary"`); the CSS below
   strips all default button chrome from buttons whose wrapper
   carries `st-key-top_n_btn_<n>` (only the Top-N buttons) and
   applies cardinal-red + gold-underline styling to the
   primary variant. Other buttons in the app are untouched
   because the scope is the `[class*="st-key-top_n_btn_"]`
   wrapper class — unique to these three widgets. */

/* Legend prose container (col 0) — strip the boxed-legend look
   so the prose reads as plain inline text on the same row. */
.heatmap-legend-inline {
    background: transparent;
    border: none;
    padding: 0;
    margin: 0;
    font-size: 12px;
    color: rgba(0, 0, 0, 0.6);
    line-height: 1.5;
}

/* "Showing top" inline label (col 1) — same muted gray as the
   legend, sized to match. */
.top-n-label {
    font-size: 12px;
    color: rgba(0, 0, 0, 0.55);
    line-height: 1.5;
    text-align: right;
    padding-right: 4px;
}

/* Top-N button shells — strip default Streamlit chrome so the
   buttons read as inline numbers. The wrapper class
   `st-key-top_n_btn_<10|20|30>` is auto-generated from each
   button's `key=` argument and is unique to these three
   widgets, so the rules below cannot bleed into any other
   button in the app.

   Specificity: `html body [class*="st-key-top_n_btn_"] button`
   is 0,1,3 — equal to the site-wide `html body .stButton >
   button` maroon rule. With `!important` on both sides and
   equal specificity, source order is the tiebreaker, and this
   block is below the maroon rule in the same _GLOBAL_CSS
   string — so this wins. */
html body [class*="st-key-top_n_btn_"] button,
html body [class*="st-key-top_n_btn_"] button:hover,
html body [class*="st-key-top_n_btn_"] [data-testid="stBaseButton-secondary"],
html body [class*="st-key-top_n_btn_"] [data-testid="stBaseButton-secondary"]:hover,
html body [class*="st-key-top_n_btn_"] [data-testid="stBaseButton-primary"],
html body [class*="st-key-top_n_btn_"] [data-testid="stBaseButton-primary"]:hover {
    background: transparent !important;
    background-color: transparent !important;
    border: none !important;
    box-shadow: none !important;
    text-shadow: none !important;
    outline: none !important;
    padding: 0 4px !important;
    margin: 0 !important;
    min-height: 0 !important;
    height: auto !important;
    border-radius: 0 !important;
    font-weight: 500 !important;
    font-size: 13px !important;
    line-height: 1.4 !important;
}
/* Inner <p>/<div> inside the button — Streamlit wraps the label
   in a markdown container; that's where the actual visible text
   lives. */
html body [class*="st-key-top_n_btn_"] button p,
html body [class*="st-key-top_n_btn_"] button div {
    color: rgba(0, 0, 0, 0.55) !important;
    font-size: 13px !important;
    font-weight: 500 !important;
    text-decoration: none !important;
    margin: 0 !important;
    padding: 0 !important;
    line-height: 1.4 !important;
    text-shadow: none !important;
}
/* Hover state (unselected) — text color flips to cardinal, background
   stays transparent. The :hover variants in the chrome-stripping rule
   above keep the maroon site-wide hover background from leaking in. */
html body [class*="st-key-top_n_btn_"] button:hover p,
html body [class*="st-key-top_n_btn_"] button:hover div {
    color: #790A26 !important;
}

/* Primary (selected) variant — cardinal red text with gold
   underline. Matches both the button itself and the inner
   <p>/<div> so the underline renders on the visible text. */
html body [class*="st-key-top_n_btn_"] button[kind="primary"],
html body [class*="st-key-top_n_btn_"] [data-testid="stBaseButton-primary"],
html body [class*="st-key-top_n_btn_"] button[kind="primary"]:hover,
html body [class*="st-key-top_n_btn_"] [data-testid="stBaseButton-primary"]:hover {
    background: transparent !important;
    background-color: transparent !important;
    border: none !important;
    box-shadow: none !important;
}
html body [class*="st-key-top_n_btn_"] button[kind="primary"] p,
html body [class*="st-key-top_n_btn_"] button[kind="primary"] div,
html body [class*="st-key-top_n_btn_"] [data-testid="stBaseButton-primary"] p,
html body [class*="st-key-top_n_btn_"] [data-testid="stBaseButton-primary"] div {
    color: #790A26 !important;
    font-weight: 500 !important;
    text-decoration: underline !important;
    text-decoration-color: #F1AB1F !important;
    text-decoration-thickness: 2px !important;
    text-underline-offset: 3px !important;
}

/* ── Top-N legend BASELINE ALIGNMENT (Issue 2 fix v3) ──────
   "Showing top" and the 10/20/30 buttons looked misaligned
   despite multiple iterations. The previous "lock all column
   children to 24 px boxes + align-items: center" approach
   failed because Streamlit 1.57.0 implements
   `st.columns(vertical_alignment="center")` as
       margin-top: auto; margin-bottom: auto
   on EACH stColumn — NOT as `align-items: center` on the
   parent stHorizontalBlock. In flex layout, `margin: auto`
   on a child ABSORBS all free space first, OVERRIDING the
   parent's `align-items`. So any `align-items` rule on the
   parent is a no-op until the columns' auto margins are
   neutralized.
   Verified by reading streamlit/streamlit 1.57.0's JS bundle
   (`src.D9MArGZj.js`): `i === a.CENTER && {marginTop:'auto',
   marginBottom:'auto'}`.
   This fix:
     1. Zero the auto margins on each stColumn (so the
        parent's align-items takes effect).
     2. Set `align-items: baseline` on the parent
        stHorizontalBlock (`:has()` scopes it to the row
        containing our Top-N buttons only).
     3. Match font-size + line-height across all three
        children (12 px / 1.5) so baselines coincide.
     4. Drop ALL the 24-px height locks from the prior
        attempt — height locks prevent flex from giving each
        wrapper its natural intrinsic height, and baseline
        alignment needs the natural baseline position, not a
        clamped one. */
[data-testid="stHorizontalBlock"]:has([class*="st-key-top_n_btn_"]) {
    align-items: baseline !important;
}
[data-testid="stHorizontalBlock"]:has([class*="st-key-top_n_btn_"])
    > [data-testid="stColumn"] {
    margin-top: 0 !important;
    margin-bottom: 0 !important;
}
.heatmap-legend-inline,
.top-n-label {
    font-size: 12px !important;
    line-height: 1.5 !important;
    margin: 0 !important;
    padding: 0 !important;
}
.top-n-label {
    text-align: right;
    padding-right: 4px !important;
}
html body [class*="st-key-top_n_btn_"] button {
    font-size: 12px !important;
    line-height: 1.5 !important;
    padding: 0 6px !important;
    min-height: 0 !important;
    height: auto !important;
}
html body [class*="st-key-top_n_btn_"] button p,
html body [class*="st-key-top_n_btn_"] button div {
    font-size: 12px !important;
    line-height: 1.5 !important;
    margin: 0 !important;
    padding: 0 !important;
}

/* ═══════════════════════════════════════════════════════
   APP HEADER STRIPE — 2 px cardinal band that sits directly
   between the dark header bar and the light content area.
   Uses the same full-bleed-minus-sidebar margin formula as
   the bar above (50% - 50vw + 160 px on each side), so the
   stripe runs edge-to-edge with the bar across the full
   content area regardless of block-container's max-width
   1480 px constraint. The cardinal stripe is the only place
   USC's cardinal appears as a background colour in the new
   design.
   ═══════════════════════════════════════════════════════ */
.app-header-stripe {
    /* 2 px USC cardinal stripe directly under the dark header bar.
       margin-bottom was 40 px which produced a chasm between the
       stripe and whatever followed (KPI cards on analytics, the
       st.info disclaimer on pre-analytics). Streamlit's
       stElementContainer wrappers prevent margin collapse, so the
       40 px wins outright over any margin-top the next element sets.
       Dropped to 8 px so it matches the 0.5rem margin-bottom on
       st.info — produces a symmetric gap above/below the
       pre-analytics disclaimer banner and tightens the analytics
       stripe → KPI cards spacing without making it cramped. */
    height: 2px !important;
    background: #790A26 !important;
    margin-top: 0 !important;
    margin-bottom: 8px !important;
    padding: 0 !important;
    border: none !important;
    position: relative !important;
}
/* Desktop full-bleed-minus-sidebar geometry: explicit width = stMain
   (100vw - 320 px sidebar), shifted left via calc margin so the left
   edge sits at the sidebar's right edge. On mobile (≤768 px) the
   sidebar is overlay (not a 320 px column), so this calc geometry
   becomes wrong — see mobile override below. */
@media (min-width: 768.01px) {
    .app-header-stripe {
        width: calc(100vw - 320px) !important;
        max-width: calc(100vw - 320px) !important;
        margin-left: calc(50% - 50vw + 160px) !important;
        margin-right: 0 !important;
    }
}
@media (max-width: 768px) {
    .app-header-stripe {
        width: 100% !important;
        max-width: 100% !important;
        margin-left: 0 !important;
        margin-right: 0 !important;
    }
}
.metrics-divider {
    /* Subtle horizontal line between the KPI card row and the
       section heading below. !important is REQUIRED to beat the
       global `hr { margin: 1.1rem 0 !important }` rule earlier in
       this stylesheet (element selector + !important would otherwise
       win over a class selector without !important).
       Top margin = 32 px (gap from KPI cards to the line); bottom
       margin = 20 px which combines with the section-heading's 12 px
       margin-top to give 32 px below the line. Result: the line is
       visually centered between the KPI cards and the section heading
       text at 32 / 32 px. */
    border: none !important;
    border-top: 1px solid rgba(0, 0, 0, 0.06) !important;
    margin: 32px 0 20px 0 !important;
    height: 0 !important;
}
/* st.info disclaimer banner — used for the forecast notice on
   analytics Daily view and the completed-orders caveat on every
   pre-analytics view. Streamlit's default vertical padding is too
   generous for single-line text. Shrink the inner padding so the
   blue background hugs the text (text size itself is unchanged).
   Apply margin-bottom only to the OUTER alert wrapper — applying
   it to the inner container too stacked the spacing and pushed
   the KPI cards way too far down. Half a rem here combines with
   Streamlit's default block gap for a modest separation. */
[data-testid="stAlert"],
[data-testid="stAlertContainer"] {
    padding-top: 0.5rem !important;
    padding-bottom: 0.5rem !important;
}
[data-testid="stAlert"] {
    margin-bottom: 0.5rem !important;
}
.status-chip {
    display: inline-block;
    background: #DCFCE7;
    color: #166534;
    font-size: 0.7rem;
    font-weight: 600;
    padding: 3px 9px;
    border-radius: 10px;
    margin-bottom: 0.4rem;
}
.status-chip.warn  { background: #FEF9C3; color: #854D0E; }
.status-chip.error { background: #FEE2E2; color: #991B1B; }
.refresh-btn button {
    background-color: #6F1828 !important;
    color: #ffffff !important;
    font-weight: 600 !important;
    border: none !important;
    border-radius: 6px !important;
    width: 100% !important;
}

div[data-testid="stDataFrame"] {
    border-radius: 8px;
    overflow: hidden;
    box-shadow: 0 1px 3px rgba(0,0,0,0.08);
}

/* ═══════════════════════════════════════════════════════
   LOGIN OVERLAY — instant hide after auth (issue #10)
   ═══════════════════════════════════════════════════════ */
#login-overlay {
    transition: opacity 0.2s ease, transform 0.2s ease;
}
#login-overlay.hiding {
    opacity: 0;
    transform: translateY(-10px);
    pointer-events: none;
}
</style>
"""


