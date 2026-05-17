"""
conftest.py — Shared fixtures for the lab-dashboard test suite.

Synthetic DataFrames cover the shapes that analytics.data /
pre_analytics.data / formatting functions expect, without hitting
real partitions or Streamlit's cache layer.
"""

from datetime import date

import pandas as pd
import pytest


@pytest.fixture
def tat_targets() -> dict[str, int]:
    """Default per-priority TAT targets in minutes."""
    return {"RT": 120, "ST": 60, "TS": 60}


@pytest.fixture
def tat_targets_specialty() -> dict[str, int]:
    """Norris Specialty's flat 48h SLA override."""
    return {"RT": 48 * 60, "ST": 48 * 60, "TS": 48 * 60}


@pytest.fixture
def tat_df() -> pd.DataFrame:
    """3-column TAT projection (what compute_tat_metrics returns).

    Layout: 4 procedures × multiple priorities × known TAT minutes.
    Includes edge cases (exact-target, just-over-target, unmapped
    priority 'NU', a NaN TAT row that should be excluded by callers).
    """
    rows = [
        # CBC w diff — mix of priorities, includes edge cases
        ("CBC w diff", "RT",  30.0),    # well within RT 120 target
        ("CBC w diff", "RT",  120.0),   # EXACTLY at target — counts with <=
        ("CBC w diff", "RT",  121.0),   # just over
        ("CBC w diff", "ST",  45.0),
        ("CBC w diff", "ST",  60.0),    # exactly at target
        ("CBC w diff", "TS",  55.0),
        ("CBC w diff", "NU",  30.0),    # unmapped priority — excluded from %
        # BMP — RT-only
        ("BMP",        "RT",  100.0),
        ("BMP",        "RT",  150.0),
        # CMP — all over target
        ("CMP",        "RT",  300.0),
        ("CMP",        "ST",  120.0),
        # Empty-result procedure
        ("Lactic Acid", "RT", None),    # NaN TAT — corner case
    ]
    df = pd.DataFrame(rows, columns=["Order Procedure", "Collection Priority", "TAT_minutes"])
    return df


@pytest.fixture
def daily_completed_df() -> pd.DataFrame:
    """One day's data (May 15, 2026) across 3 procedures and 4 hours.

    Shape: what load_analytics_data returns (post-compute_metrics)
    for the Daily Completed view.
    """
    d = date(2026, 5, 15)
    rows = []
    # 8-9 AM: procedure A heavy
    for _ in range(20):
        rows.append({"complete_date": d, "hour": 8,  "Order Procedure": "CBC w diff", "Complete Volume": 1.0})
    for _ in range(15):
        rows.append({"complete_date": d, "hour": 9,  "Order Procedure": "CBC w diff", "Complete Volume": 1.0})
    # 10-11 AM: procedure B
    for _ in range(10):
        rows.append({"complete_date": d, "hour": 10, "Order Procedure": "BMP",        "Complete Volume": 1.0})
    for _ in range(8):
        rows.append({"complete_date": d, "hour": 11, "Order Procedure": "BMP",        "Complete Volume": 1.0})
    # 12 PM: procedure C
    for _ in range(5):
        rows.append({"complete_date": d, "hour": 12, "Order Procedure": "CMP",        "Complete Volume": 1.0})
    return pd.DataFrame(rows)


@pytest.fixture
def monthly_completed_df() -> pd.DataFrame:
    """A small monthly dataset: May 1-3 2026, 2 procedures across a few hours.

    Volume per (date, hour, procedure):
      May 1, 8 AM, CBC w diff : 10
      May 1, 9 AM, CBC w diff : 12
      May 2, 8 AM, CBC w diff : 8
      May 2, 9 AM, BMP        : 6
      May 3, 10 AM, BMP       : 4
    """
    rows = []
    def add(d, hour, proc, n):
        for _ in range(n):
            rows.append({
                "complete_date": d, "hour": hour,
                "Order Procedure": proc, "Complete Volume": 1.0,
            })
    add(date(2026, 5, 1), 8, "CBC w diff", 10)
    add(date(2026, 5, 1), 9, "CBC w diff", 12)
    add(date(2026, 5, 2), 8, "CBC w diff", 8)
    add(date(2026, 5, 2), 9, "BMP",        6)
    add(date(2026, 5, 3), 10, "BMP",       4)
    return pd.DataFrame(rows)
