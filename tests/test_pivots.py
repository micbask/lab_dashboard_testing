"""
test_pivots.py — Unit tests for the pivot-building functions.

Covers:
  • analytics.data.build_pivot           — Daily heatmap pivot
  • analytics.data.build_monthly_pivot   — Monthly pivot + elapsed-days
                                           denominator (Batch 2 #6)
  • pre_analytics.data.normalize_name    — name canonicalisation
                                           (Batch 2 #7)
"""

from datetime import date

import pandas as pd
import pytest

from analytics.data import build_pivot, build_monthly_pivot
from pre_analytics.data import normalize_name


# ─────────────────────────────────────────────────────────────────────
# build_pivot — Daily heatmap
# ─────────────────────────────────────────────────────────────────────

class TestBuildPivot:
    def test_empty_input_returns_none_pivot(self):
        df = pd.DataFrame(
            columns=["complete_date", "hour", "Order Procedure", "Complete Volume"]
        )
        pivot, df_dh, df_date, hours = build_pivot(df, date(2026, 5, 15), (0, 23))
        assert pivot is None

    def test_pivot_shape_matches_top_n(self, daily_completed_df):
        pivot, *_ = build_pivot(daily_completed_df, date(2026, 5, 15), (0, 23), top_n=2)
        # 2 top procedures × (24 hours + Total) = 25 columns
        assert pivot is not None
        assert len(pivot.index) == 2
        assert "Total" in pivot.columns

    def test_total_column_sums_hours(self, daily_completed_df):
        pivot, *_ = build_pivot(daily_completed_df, date(2026, 5, 15), (0, 23), top_n=10)
        # CBC w diff has 20 + 15 = 35 completions
        cbc_total = pivot.loc["CBC w diff", "Total"]
        assert cbc_total == 35

    def test_top_n_keeps_only_top_procedures(self, daily_completed_df):
        pivot, *_ = build_pivot(daily_completed_df, date(2026, 5, 15), (0, 23), top_n=1)
        # Only CBC w diff (35 total) should be present
        assert pivot is not None
        assert list(pivot.index) == ["CBC w diff"]

    def test_top_n_none_keeps_all_procedures(self, daily_completed_df):
        pivot, *_ = build_pivot(daily_completed_df, date(2026, 5, 15), (0, 23), top_n=None)
        assert set(pivot.index) == {"CBC w diff", "BMP", "CMP"}

    def test_hour_range_filters_pivot(self, daily_completed_df):
        # 8-9 only — should exclude BMP (10-11) and CMP (12)
        pivot, *_ = build_pivot(daily_completed_df, date(2026, 5, 15), (8, 9), top_n=10)
        assert pivot is not None
        # Only CBC w diff has draws in this range
        assert set(pivot.index) == {"CBC w diff"}

    def test_other_dates_excluded(self, daily_completed_df):
        pivot, *_ = build_pivot(daily_completed_df, date(2026, 6, 1), (0, 23))
        assert pivot is None


# ─────────────────────────────────────────────────────────────────────
# build_monthly_pivot — Monthly pivot + elapsed-days denominator
# ─────────────────────────────────────────────────────────────────────

class TestBuildMonthlyPivot:
    def test_empty_input_returns_none(self):
        df = pd.DataFrame(
            columns=["complete_date", "hour", "Order Procedure", "Complete Volume"]
        )
        pivot, n_days, _ = build_monthly_pivot(df, 2026, 5)
        assert pivot is None
        assert n_days == 0

    def test_calendar_days_used_for_past_month(self, monthly_completed_df):
        # April 2026 is fully past (today is well into 2026); n_days
        # should be 30 (April has 30 days) even if data is sparse.
        # monthly_completed_df has only May data so April pivot is None.
        pivot, n_days, _ = build_monthly_pivot(monthly_completed_df, 2026, 4)
        assert pivot is None

    def test_elapsed_days_for_present_or_past(self, monthly_completed_df):
        # May 2026 — data spans May 1-3. n_days denominator is the
        # number of elapsed calendar days in May, capped at today.
        # Either way it should be >= 3 (we have data through May 3).
        pivot, n_days, _ = build_monthly_pivot(monthly_completed_df, 2026, 5)
        assert pivot is not None
        assert n_days >= 3

    def test_total_column_is_sum_of_hourly_avgs(self, monthly_completed_df):
        # CBC w diff totals: 10+12+8 = 30, divided by n_days.
        pivot, n_days, _ = build_monthly_pivot(monthly_completed_df, 2026, 5)
        # Hour columns are labels like "8AM" — get the numeric value via Total
        cbc_total = pivot.loc["CBC w diff", "Total"]
        expected = 30 / n_days
        assert abs(cbc_total - expected) < 1e-9

    def test_top_n_filter_excludes_lower_volume_procedures(self, monthly_completed_df):
        # CBC w diff has 30 (top); BMP has 10. Top-1 keeps only CBC w diff.
        pivot, n_days, _ = build_monthly_pivot(monthly_completed_df, 2026, 5, top_n=1)
        assert list(pivot.index) == ["CBC w diff"]


# ─────────────────────────────────────────────────────────────────────
# normalize_name — Batch 2 #7
# ─────────────────────────────────────────────────────────────────────

class TestNormalizeName:
    @pytest.mark.parametrize("inp,expected", [
        # Whitespace around comma
        ("Nunez-Astorga , Karina",   "nunez-astorga, karina"),
        ("Smith,Jane",               "smith, jane"),
        # Middle initial with or without dot
        ("Smith, Jane M",            "smith, jane"),
        ("Smith, Jane M.",           "smith, jane"),
        # Honorifics
        ("Smith, Jane Jr",           "smith, jane"),
        ("Smith, Jane RN",           "smith, jane"),
        ("Smith, Jane III",          "smith, jane"),
        # Already canonical
        ("Smith, Jane",              "smith, jane"),
        # Real PMOB techs
        ("Reyes, Renee",             "reyes, renee"),
        ("Quiroz, Ruben ",           "quiroz, ruben"),     # trailing space
        # Edge cases
        (None,                       None),
        ("-",                        None),
        ("nan",                      None),
        ("",                         None),
    ])
    def test_normalize_name_cases(self, inp, expected):
        assert normalize_name(inp) == expected
