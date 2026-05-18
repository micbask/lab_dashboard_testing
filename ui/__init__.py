"""
ui — UI helper package (split from the old ui_components.py monolith).

Modules:
  • css              — global CSS injection + sidebar resize-kill JS
  • cards            — metric cards, status chips
  • header           — dashboard banner (USC strip)
  • data_management  — Data Management sidebar expander

Public API is re-exported here so callers can do
`from ui import inject_css, metric_card, ...`. The legacy
ui_components shim re-exports the same names so existing
`from ui_components import metric_card` imports keep working.
"""

from ui.css import inject_css, inject_sidebar_resize_kill  # noqa: F401
from ui.cards import metric_card, status_chip  # noqa: F401
from ui.header import render_header  # noqa: F401
from ui.data_management import render_data_management_sidebar  # noqa: F401
