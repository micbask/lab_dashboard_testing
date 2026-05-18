"""
procedure_aliases.py — Shared procedure-name normalisation rules.

The dashboard normalises raw procedure names from the source files in
two passes: collapse non-breaking-space variants of the CBC w/diff name
to a single canonical form, then map verbose canonical names to short
display aliases. Both `parsing.clean_procedure_names` (used at upload
time) and `storage.load_filtered_data` (used at read time) rely on
identical pass-1 and pass-2 rules. The same logic is mirrored in the
standalone ingest script at `scripts/email_ingest.py`.

This module centralises the rule data — the two `clean_procedure_names`
implementations import the same dicts so the rules can't drift apart
silently across files. The function bodies stay duplicated (parsing.py
imports from procedure_aliases; email_ingest.py is intentionally kept
free of repo-internal imports beyond this one tiny pure-data module so
the script can still run as a standalone GitHub Action).
"""

# Pass 1 — collapse \xa0 (non-breaking space) variants of the CBC w/diff
# name to a single canonical double-space form. The upstream source
# emits multiple flavours of this name depending on export pathway;
# squash them all here so downstream filtering / dedup is predictable.
#
# Stored as a list of (source, replacement) tuples rather than a dict
# because all source keys share the same replacement and order matters
# only for documentation.
PROCEDURE_WHITESPACE_NORMALIZATIONS: list[tuple[str, str]] = [
    (
        "Complete Blood Count With Auto\xa0 Differen",
        "Complete Blood Count With Auto  Differen",
    ),
    (
        "Complete Blood Count With Auto\xa0Differen",
        "Complete Blood Count With Auto  Differen",
    ),
]

# Pass 2 — verbose canonical procedure names → short display aliases the
# dashboards use everywhere. Keys must match the form produced by
# pass 1 above (post-normalisation). The keys don't overlap as
# substrings so order doesn't matter for correctness.
PROCEDURE_DISPLAY_ALIASES: dict[str, str] = {
    "Complete Blood Count With Auto  Differen": "CBC w diff",
    "Complete Blood Count NO Auto Differentia": "CBC no diff",
    "Comprehensive Metabolic Panel":            "CMP",
    "Basic Metabolic Panel":                    "BMP",
}
