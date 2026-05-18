"""
ui_components.py — Backward-compat shim for the ui package.

This module used to hold ~2100 lines of CSS + UI helpers. In Batch 5
Phase 3 the contents were split into a ui/ package:
  ui.css              inject_css, inject_sidebar_resize_kill
  ui.cards            metric_card, status_chip
  ui.header           render_header
  ui.data_management  render_data_management_sidebar

This file is now a thin re-export so existing
`from ui_components import metric_card, render_header, ...`
imports across the codebase keep working without churn.
"""

from ui.css import inject_css, inject_sidebar_resize_kill  # noqa: F401
from ui.cards import metric_card, status_chip  # noqa: F401
from ui.header import render_header  # noqa: F401
from ui.data_management import render_data_management_sidebar  # noqa: F401
