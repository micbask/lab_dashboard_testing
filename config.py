"""
config.py — Application constants and site configuration.

All per-site configuration, resource remaps, exclusions, and shared
helpers live here.  To add a new lab site, add one entry to SITE_CONFIG.
"""

# ═════════════════════════════════════════════════════════════════════════════
# BRANDING COLOURS
# ═════════════════════════════════════════════════════════════════════════════
NAVY   = "#6F1828"   # USC Maroon
GOLD   = "#EDC153"   # USC Gold
STEEL  = "#57121f"   # Dark USC Maroon
WHITE  = "#FFFFFF"
LIGHT  = "#F4F6FA"
MUTED  = "#6B7280"
BORDER = "#D1D5DB"

# ═════════════════════════════════════════════════════════════════════════════
# SITE CONFIGURATION
# ═════════════════════════════════════════════════════════════════════════════
# ─── TAT target defaults ───────────────────────────────────────────────
# Per-priority service-level targets in minutes. The "% within target"
# stat in the TAT view is computed against the row's own priority
# target: RT vs the 2h SLA, ST/TS vs 1h. Bench-specific overrides
# live in SITE_CONFIG[bench]["tat_targets"] below.
DEFAULT_TAT_TARGETS: dict[str, int] = {
    "RT": 120,  # Routine — 2-hour service level
    "ST": 60,   # Stat — 1-hour
    "TS": 60,   # Time Study — 1-hour
}

# Core panel — the 5 procedures the TAT view defaults to selecting
# on benches whose `use_core_panel_defaults` is True. Listed in
# fixed clinical-priority order so the table reads RT-first.
CORE_PANEL_DEFAULTS: list[str] = [
    "CBC w diff", "CBC no diff", "BMP", "CMP", "Lactic Acid",
]

# ─── Per-bench site configuration ──────────────────────────────────────
# To add a new lab bench, add ONE entry below. Every downstream consumer
# (analytics radio, pre-analytics radio if applicable, TAT targets, core
# panel defaults, resource picker, forecast retraining) reads from this
# dict — there are no other places to edit.
#
# Keys:
#   resources                 list[str]    — bench's service-resource list
#   vmax                      int          — heatmap colour-scale high end
#   short_label               str          — what the analytics radio shows
#   tat_targets               dict|None    — per-priority overrides; None
#                                            means "use DEFAULT_TAT_TARGETS"
#   use_core_panel_defaults   bool         — TAT view defaults to the
#                                            5-procedure core panel when
#                                            True, else top-5-by-volume
SITE_CONFIG: dict[str, dict] = {
    "Keck Core": {
        "resources": [
            "Keck Abbott DI", "Keck Coagulation", "Keck Cobas",
            "Keck HEME Orders", "Keck IRIS", "Keck ISED", "Keck SmartLyte A",
            "Keck TEG 5000", "Keck Urinalysis", "USC Manual Coagulation Bench",
            "USC Manual Hematology Bench", "USC Manual Urinalysis Bench",
            "USC Serology Routine Bench",
        ],
        "vmax": 50,
        "short_label": "Keck",
        "tat_targets": None,
        "use_core_panel_defaults": True,
    },
    "Norris Core": {
        "resources": [
            "NCH Coagulation", "NCH COBAS", "NCH HEME Orders", "NCH IRIS",
            "NCI Manual Chemistry Bench", "NCI Manual Hematology Bench",
            "NCI Stem Cell Bench", "NCH Cobas PRO A", "NCH Cobas PRO B",
            "NCH GEM 4000 H", "NCH GEM 4000 I",
        ],
        "vmax": 30,
        "short_label": "Norris",
        "tat_targets": None,
        "use_core_panel_defaults": True,
    },
    "Norris Specialty": {
        "resources": [
            "NCH DS2 A", "NCH HydraSys", "NCH PFA 100", "NCH Tosoh G8",
            "NCI Manual Flow Bench", "NCI Manual Verify Now Bench",
        ],
        "vmax": 20,
        "short_label": "Specialty",
        # Send-out work: 48h flat SLA across all priorities.
        "tat_targets": {"RT": 48 * 60, "ST": 48 * 60, "TS": 48 * 60},
        "use_core_panel_defaults": False,
    },
    "PMOB": {
        "resources": [
            "PAS Cellavision", "PAS COBAS", "PAS Cobas U411",
            "PAS HEME Man Diff", "PAS HEME Orders", "PAS HEME Results",
            "PAS IRIS", "PAS XN2000 1", "PAS XN2000 2", "PAS Beckman LH500",
            "PAS Manual Chemistry Bench", "PAS Manual Hematology Bench",
            "PAS Manual Urinalysis Bench",
        ],
        "vmax": 30,
        "short_label": "PMOB",
        "tat_targets": None,
        "use_core_panel_defaults": True,
    },
}

# Derived helpers
DEFAULT_RESOURCES: dict[str, list] = {k: v["resources"]    for k, v in SITE_CONFIG.items()}
VMAX:              dict[str, int]  = {k: v["vmax"]         for k, v in SITE_CONFIG.items()}
ALL_RESOURCES:     list[str]       = sorted({r for v in SITE_CONFIG.values() for r in v["resources"]})
MAP_TYPES:         list[str]       = list(SITE_CONFIG.keys())

# Radio labels for the Testing Bench picker. {short_label → full bench name}.
BENCH_LABEL_TO_VALUE: dict[str, str] = {
    v["short_label"]: k for k, v in SITE_CONFIG.items()
}

# {bench → resolved per-priority targets dict}. Always present — bench
# entries with `tat_targets: None` resolve to DEFAULT_TAT_TARGETS.
TAT_TARGET_OVERRIDES: dict[str, dict[str, int]] = {
    k: (v["tat_targets"] or DEFAULT_TAT_TARGETS)
    for k, v in SITE_CONFIG.items()
}

# Benches that should default their TAT procedure filter to the core panel.
BENCHES_USING_CORE_PANEL: set[str] = {
    k for k, v in SITE_CONFIG.items() if v["use_core_panel_defaults"]
}


def get_tat_targets(bench: str | None) -> dict[str, int]:
    """Resolve the per-priority TAT targets for `bench`.

    Returns a fresh dict so callers can mutate without affecting the
    module-level data. Unknown benches (or None) fall back to defaults.
    """
    if bench and bench in TAT_TARGET_OVERRIDES:
        return dict(TAT_TARGET_OVERRIDES[bench])
    return dict(DEFAULT_TAT_TARGETS)


# ─── Pre-analytics location list ───────────────────────────────────────
# Locations don't map 1:1 to benches: HC3 is a phlebotomy-only location
# with no analytics bench, and PMOB has both. Keep the list explicit
# until the pre-analytics side grows enough config to deserve its own
# dict structure.
PRE_ANALYTICS_LOCATIONS: list[str] = ["Keck", "Norris", "HC3", "PMOB"]

# Procedures always excluded from Analytics data scope (Completed,
# In-Lab, TAT) live in analytics/filters.py as EXCLUDED_PROCEDURES.

# Resource remaps: {(order_procedure, old_resource): new_resource}
RESOURCE_REMAPS: dict[tuple[str, str], str] = {
    ("Kappa/Lambda Free Light Chains Panel", "NCH COBAS"):    "NCI Manual Flow Bench",
    ("Manual Diff",                          "Keck HEME Orders"): "NCH HEME Orders",
}

# ═════════════════════════════════════════════════════════════════════════════
# COLUMN DEFINITIONS
# ═════════════════════════════════════════════════════════════════════════════

# Full set of columns to preserve from source files (if present)
ALL_SOURCE_COLUMNS: list[str] = [
    "Facility",
    "Patient Location",
    "Collection Priority",
    "Accession Nbr - Formatted",
    "Order Procedure",
    "Performing Service Resource",
    "Date/Time - Order",
    "Date/Time - Drawn",
    "Date/Time - In Lab",
    "Date/Time - Complete",
    "Drawn Tech",
    "Drawn Tech - Position",
    "Order Status",
    "Complete Volume",
]

# Columns that should be forward-filled within accession clusters
FORWARD_FILL_COLS: list[str] = [
    "Accession Nbr - Formatted",
    "Patient Location",
    "Facility",
]

# All datetime columns for normalization
DATETIME_COLUMNS: list[str] = [
    "Date/Time - Order",
    "Date/Time - Drawn",
    "Date/Time - In Lab",
    "Date/Time - Complete",
]

# ═════════════════════════════════════════════════════════════════════════════
# HOUR HELPERS
# ═════════════════════════════════════════════════════════════════════════════

def _hour_label(h: int) -> str:
    hr12   = 12 if h % 12 == 0 else h % 12
    suffix = "AM" if h < 12 else "PM"
    return f"{hr12}{suffix}"

HOUR_LABELS   = {h: _hour_label(h) for h in range(24)}
LABEL_TO_HOUR = {v: k for k, v in HOUR_LABELS.items()}

# ═════════════════════════════════════════════════════════════════════════════
# FORECAST
# ═════════════════════════════════════════════════════════════════════════════
FORECAST_HORIZON = 14  # forecast days ahead

# ═════════════════════════════════════════════════════════════════════════════
# TIMEZONE
# ═════════════════════════════════════════════════════════════════════════════
#
# All lab timestamps are recorded in local time. Until this constant was
# introduced the dashboard stored and computed everything tz-naive, which
# silently mis-handled the two DST transition days per year:
#   • Spring-forward: 2:00-3:00 AM doesn't exist → a "2:30" timestamp in
#     that hour gets parsed as something nonsensical or becomes NaT.
#   • Fall-back: 1:00-2:00 AM occurs twice → "1:30" is ambiguous.
#
# parsing.add_derived_columns localizes incoming timestamps to this
# zone at ingest; storage._parquet_bytes_to_df does the same at read
# time so partitions written before the localize-at-ingest fix
# (tz-naive on disk) still surface as tz-aware in memory and the
# dashboard sees uniform dtypes regardless of partition age.
LAB_TZ = "America/Los_Angeles"

# ═════════════════════════════════════════════════════════════════════════════
# PARTITION PATHS
# ═════════════════════════════════════════════════════════════════════════════
PARTITION_DIR = "data/partitions"
PARTITION_INDEX_PATH = "data/partition_index.json"
LEGACY_PARQUET_PATH = "data/lab_data.parquet"
