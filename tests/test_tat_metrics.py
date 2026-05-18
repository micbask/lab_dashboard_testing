"""
test_tat_metrics.py — Unit tests for TAT-related pure functions.

Covers:
  • analytics.data.compute_tat_metrics — column-projection edge cases
  • analytics.data.build_tat_table — per-priority math, % within target,
    unmapped-priority handling, the new `<=` operator semantics
  • config.get_tat_targets — bench-aware target resolution
  • formatting helpers — TAT / pct / range formatting
"""

import math

import pandas as pd
import pytest

from analytics.data import build_tat_table, compute_tat_metrics
from config import (
    DEFAULT_TAT_TARGETS,
    get_tat_targets,
    TAT_TARGET_OVERRIDES,
)
from formatting import (
    format_tat, format_tat_compact, format_pct, format_range,
)


# ─────────────────────────────────────────────────────────────────────
# compute_tat_metrics
# ─────────────────────────────────────────────────────────────────────

class TestComputeTatMetrics:
    def test_empty_input_returns_empty_3col_frame(self):
        result = compute_tat_metrics(pd.DataFrame())
        assert result.empty
        assert list(result.columns) == [
            "Order Procedure", "Collection Priority", "TAT_minutes",
        ]

    def test_none_input_returns_empty_3col_frame(self):
        result = compute_tat_metrics(None)
        assert result.empty

    def test_missing_tat_minutes_column_returns_empty(self):
        # Frame has rows but no TAT_minutes — shouldn't happen post-loader
        # but the function must defend against it.
        df = pd.DataFrame({"Order Procedure": ["x"], "Collection Priority": ["RT"]})
        result = compute_tat_metrics(df)
        assert result.empty

    def test_projects_three_columns_and_drops_extras(self, tat_df):
        result = compute_tat_metrics(tat_df)
        assert list(result.columns) == [
            "Order Procedure", "Collection Priority", "TAT_minutes",
        ]
        assert len(result) == len(tat_df)


# ─────────────────────────────────────────────────────────────────────
# build_tat_table — the most intricate TAT math
# ─────────────────────────────────────────────────────────────────────

class TestBuildTatTable:
    def test_row_order_preserves_selected_procedures(self, tat_df, tat_targets):
        # Caller passes a specific order; result must respect it.
        order = ["CMP", "BMP", "CBC w diff"]
        result = build_tat_table(tat_df, order, tat_targets)
        assert result[("Procedure", "Procedure")].tolist() == order

    def test_exactly_at_target_counts_with_le_operator(self, tat_targets):
        # A TAT of exactly 60 minutes vs the ST 60-minute target
        # MUST count as "within target" under the new `<=` semantics
        # (Batch 4 #23). Under the old `<` it would have been a miss.
        df = pd.DataFrame([
            ("X", "ST", 60.0),
            ("X", "ST", 60.0),
            ("X", "ST", 60.0),
        ], columns=["Order Procedure", "Collection Priority", "TAT_minutes"])
        result = build_tat_table(df, ["X"], tat_targets)
        st_pct = result.loc[0, ("ST", "% within target")]
        assert st_pct == 100.0, "samples at exactly the target should count as within"

    def test_unmapped_priority_excluded_from_all_pct_denominator(self, tat_targets):
        # 3 known RT (all under 120), 1 unmapped 'NU'. Without the fix
        # the NU sample would count as a miss, giving 75% compliance;
        # with the fix it's excluded, giving 100%.
        df = pd.DataFrame([
            ("X", "RT", 30.0),
            ("X", "RT", 60.0),
            ("X", "RT", 90.0),
            ("X", "NU", 30.0),   # unmapped — should be excluded from %
        ], columns=["Order Procedure", "Collection Priority", "TAT_minutes"])
        result = build_tat_table(df, ["X"], tat_targets)
        all_pct = result.loc[0, ("All", "% within target")]
        all_n   = result.loc[0, ("All",  "n")]
        assert all_n == 4, "n should reflect every sample"
        assert all_pct == 100.0, "unmapped priority must not silently count as a miss"

    def test_per_priority_n_and_mean(self, tat_df, tat_targets):
        result = build_tat_table(tat_df, ["CBC w diff"], tat_targets)
        # CBC w diff has 3 RT, 2 ST, 1 TS (+ 1 NU which doesn't count in RT/ST/TS)
        assert result.loc[0, ("RT", "n")] == 3
        assert result.loc[0, ("ST", "n")] == 2
        assert result.loc[0, ("TS", "n")] == 1
        # RT mean of [30, 120, 121]
        assert math.isclose(result.loc[0, ("RT", "Mean")], (30 + 120 + 121) / 3)

    def test_priority_pct_within_target(self, tat_df, tat_targets):
        # RT targets = 120. CBC w diff RT TATs are [30, 120, 121].
        # With <= operator: 30 ≤ 120 ✓, 120 ≤ 120 ✓, 121 ≤ 120 ✗ → 2/3 = 66.67%
        result = build_tat_table(tat_df, ["CBC w diff"], tat_targets)
        rt_pct = result.loc[0, ("RT", "% within target")]
        assert math.isclose(rt_pct, 200.0 / 3.0)

    def test_min_max_per_priority(self, tat_df, tat_targets):
        result = build_tat_table(tat_df, ["CBC w diff"], tat_targets)
        assert result.loc[0, ("RT", "Min")] == 30.0
        assert result.loc[0, ("RT", "Max")] == 121.0

    def test_empty_priority_group_returns_nones(self, tat_targets):
        # BMP has no ST or TS samples in tat_df → those groups are None
        df = pd.DataFrame([
            ("BMP", "RT", 100.0),
        ], columns=["Order Procedure", "Collection Priority", "TAT_minutes"])
        result = build_tat_table(df, ["BMP"], tat_targets)
        assert result.loc[0, ("ST", "n")] is None
        assert result.loc[0, ("TS", "Mean")] is None

    def test_empty_input_returns_empty_frame_with_multiindex(self, tat_targets):
        result = build_tat_table(pd.DataFrame(), ["X"], tat_targets)
        assert result.empty
        assert ("RT", "n") in result.columns

    def test_targets_arg_is_required(self, tat_df):
        # Batch 4 #22 — removed the default value so a future caller
        # can't silently use the wrong targets.
        with pytest.raises(TypeError):
            build_tat_table(tat_df, ["CBC w diff"])  # type: ignore[call-arg]

    def test_specialty_targets_loosen_compliance(self, tat_df, tat_targets_specialty):
        # Norris Specialty's 48h SLA means EVERY CBC w diff RT sample
        # (max TAT 121 min) is well within target.
        result = build_tat_table(tat_df, ["CBC w diff"], tat_targets_specialty)
        rt_pct = result.loc[0, ("RT", "% within target")]
        assert rt_pct == 100.0


# ─────────────────────────────────────────────────────────────────────
# get_tat_targets — bench resolution
# ─────────────────────────────────────────────────────────────────────

class TestGetTatTargets:
    def test_unknown_bench_returns_defaults(self):
        result = get_tat_targets("nonexistent bench")
        assert result == DEFAULT_TAT_TARGETS

    def test_none_bench_returns_defaults(self):
        assert get_tat_targets(None) == DEFAULT_TAT_TARGETS

    def test_specialty_bench_returns_48h(self):
        result = get_tat_targets("Norris Specialty")
        assert result == {"RT": 2880, "ST": 2880, "TS": 2880}

    def test_returns_fresh_dict_not_module_reference(self):
        # Caller mutation must not leak back into TAT_TARGET_OVERRIDES.
        a = get_tat_targets("Norris Specialty")
        a["RT"] = 999
        b = get_tat_targets("Norris Specialty")
        assert b["RT"] == 2880

    def test_keck_core_uses_defaults(self):
        # Keck Core has tat_targets: None in SITE_CONFIG, so the
        # override map should still resolve to DEFAULT_TAT_TARGETS.
        result = get_tat_targets("Keck Core")
        assert result == DEFAULT_TAT_TARGETS

    def test_all_benches_present_in_override_map(self):
        from config import MAP_TYPES
        for bench in MAP_TYPES:
            assert bench in TAT_TARGET_OVERRIDES, f"{bench} missing"


# ─────────────────────────────────────────────────────────────────────
# Formatting helpers
# ─────────────────────────────────────────────────────────────────────

class TestFormatters:
    @pytest.mark.parametrize("minutes,expected", [
        (None, "-"),
        (float("nan"), "-"),
        (0, "0m"),
        (45, "45m"),
        (60, "1h 0m"),
        (65, "1h 5m"),
        (354, "5h 54m"),
        (1440, "24h 0m"),
    ])
    def test_format_tat(self, minutes, expected):
        assert format_tat(minutes) == expected

    @pytest.mark.parametrize("minutes,expected", [
        (None, "-"),
        (0, "0m"),
        (45, "45m"),
        (60, "1h"),
        (354, "5h54m"),
    ])
    def test_format_tat_compact(self, minutes, expected):
        assert format_tat_compact(minutes) == expected

    @pytest.mark.parametrize("pct,expected", [
        (None, "-"),
        (float("nan"), "-"),
        (0.0, "0.0%"),
        (66.6667, "66.7%"),
        (100.0, "100.0%"),
    ])
    def test_format_pct(self, pct, expected):
        assert format_pct(pct) == expected

    @pytest.mark.parametrize("lo,hi,expected", [
        (None, 60.0, "-"),
        (8.0, None, "-"),
        (8.0, 354.0, "8m-5h54m"),
        (60.0, 60.0, "1h-1h"),
    ])
    def test_format_range(self, lo, hi, expected):
        assert format_range(lo, hi) == expected
