"""
config.py — Application constants and site configuration.

All per-site configuration, resource remaps, exclusions, and shared
helpers live here.  To add a new lab site, add one entry to SITE_CONFIG.
"""

from copy import deepcopy

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
    },
    "Norris Core": {
        "resources": [
            "NCH Coagulation", "NCH COBAS", "NCH HEME Orders", "NCH IRIS",
            "NCI Manual Chemistry Bench", "NCI Manual Hematology Bench",
            "NCI Stem Cell Bench", "NCH Cobas PRO A", "NCH Cobas PRO B",
            "NCH GEM 4000 H", "NCH GEM 4000 I",
        ],
        "vmax": 30,
    },
    "Norris Specialty": {
        "resources": [
            "NCH DS2 A", "NCH HydraSys", "NCH PFA 100", "NCH Tosoh G8",
            "NCI Manual Flow Bench", "NCI Manual Verify Now Bench",
        ],
        "vmax": 20,
    },
}

# Derived helpers
DEFAULT_RESOURCES: dict[str, list] = {k: v["resources"] for k, v in SITE_CONFIG.items()}
VMAX:              dict[str, int]  = {k: v["vmax"]      for k, v in SITE_CONFIG.items()}
ALL_RESOURCES:     list[str]       = sorted({r for v in SITE_CONFIG.values() for r in v["resources"]})
MAP_TYPES:         list[str]       = list(SITE_CONFIG.keys())

# Procedures always excluded from heatmaps
EXCLUDE_PROCS: set[str] = {
    "Glomerular Filtration Rate Estimated",
    ".Diff Auto -",
    "Manual Diff-",
}

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

# Legacy minimal required columns (for backward compat with old parquets)
LEGACY_REQUIRED_COLS: set[str] = {
    "Performing Service Resource",
    "Order Procedure",
    "Date/Time - Complete",
    "Date/Time - In Lab",
    "Complete Volume",
}

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
# PARTITION PATHS
# ═════════════════════════════════════════════════════════════════════════════
PARTITION_DIR = "data/partitions"
PARTITION_INDEX_PATH = "data/partition_index.json"
LEGACY_PARQUET_PATH = "data/lab_data.parquet"
