"""
ui/cards.py — Metric cards and status chips.

metric_card / status_chip are HTML-returning helpers callers feed
into st.markdown. _is_long_kpi_value is a layout heuristic used by
metric_card to switch to a smaller font when the value would
otherwise wrap awkwardly.
"""

import pandas as pd
import streamlit as st


# ═════════════════════════════════════════════════════════════════════════════
# UI HELPER FUNCTIONS
# ═════════════════════════════════════════════════════════════════════════════

def _is_long_kpi_value(v: str) -> bool:
    """Heuristic: does a KPI value need the smaller-font card style?

    Short values (numbers, "9 AM"/"6 PM" hour labels, percentages,
    short tags) keep the 22 px / weight-500 main-value treatment.
    Long values (procedure names like "Magnesium Plasma/Serum",
    "Comprehensive Metabolic Panel…") get 14 px / line-height 1.2
    so they wrap gracefully without breaking row-height alignment.
    """
    s = (v or "").strip()
    if len(s) <= 6:
        return False
    if " AM" in s.upper() or " PM" in s.upper():
        return False
    if all(c.isdigit() or c in ".,%-+:" for c in s):
        return False
    return True


def metric_card(label: str, value: str, sub: str = "", accent: bool = False) -> str:
    """Return HTML for a KPI metric card.

    Uniform style for every card — `accent` is kept as a no-op
    parameter for backwards compatibility with existing call sites
    that pass `accent=True`; the new design has no per-card accent
    treatment (all cards share the same neutral white fill + thin
    border, no coloured top stripe). Long values are auto-detected
    via `_is_long_kpi_value` and rendered at a smaller font so cards
    in the same row stay equal-height.
    """
    _ = accent  # backwards compat — no-op in the new design
    long_cls = " metric-card-long" if _is_long_kpi_value(str(value)) else ""
    sub_html = f'<div class="sub">{sub}</div>' if sub else ""
    return (
        f'<div class="metric-card{long_cls}">'
        f'<div class="label">{label}</div>'
        f'<div class="value">{value}</div>'
        f'{sub_html}'
        f'</div>'
    )



def status_chip(text: str, level: str = "ok") -> None:
    """Render a small coloured status chip (level: 'ok', 'warn', or 'error')."""
    cls = {"ok": "status-chip", "warn": "status-chip warn", "error": "status-chip error"}
    st.markdown(
        f'<div class="{cls.get(level, "status-chip")}">{text}</div>',
        unsafe_allow_html=True,
    )


