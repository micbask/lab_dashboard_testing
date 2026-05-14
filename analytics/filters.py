"""Data-scope filter constants shared across the Analytics views
(Completed, In-Lab, TAT). Pre-Analytics has its own data domain
(draws / phlebotomy) and uses its own filter set — do not import
from there.

`EXCLUDED_PROCEDURES` is also referenced by `forecasting.py` indirectly
via `config.EXCLUDE_PROCS` (which is now an alias defined in
`config.py` that re-exports this set). When forecasting is migrated
to import directly from this module, the alias can be removed.
"""

EXCLUDED_PROCEDURES: frozenset[str] = frozenset({
    # GFR is a derived/calculated result, not a performed test —
    # excluding it prevents counting it as bench workload.
    "Glomerular Filtration Rate Estimated",
    # Differential auto-results: produced by analyzer firmware on
    # hematology runs, not separately performed.
    ".Diff Auto",
    ".Diff Auto -",
    # Manual differential follow-up: counted in the parent
    # Hematology run.
    "Manual Diff-",
})
