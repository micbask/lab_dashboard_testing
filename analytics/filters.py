"""Data-scope filter constants shared across the Analytics views
(Completed, In-Lab, TAT) and the forecast trainer. Pre-Analytics has
its own data domain (draws / phlebotomy) and uses its own filter set;
do not import from there.
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
