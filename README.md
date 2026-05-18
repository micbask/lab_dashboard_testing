# Lab Productivity Dashboard

A Streamlit app for USC Keck / Norris Clinical Laboratory operational analytics.
Two dashboards backed by the same partitioned Parquet store:

- **Analytics** — completed volume, in-lab activity, turnaround-time (TAT)
  service-level tracking, and 14-day Prophet forecasts per bench.
- **Pre-Analytics** — phlebotomy draw heatmaps per shift / tech, weekday
  patterns, hourly volume bars.

Used daily by lab managers and supervisors. Production data is the lab's
real export; treat it accordingly.

---

## Status & access

- **Live app:** `<fill in Streamlit Cloud URL>`
- **Access:** gated by `app_password` (see [Secrets](#secrets)); admin
  actions inside Data Management require an additional `admin_password`.
- **Hosting:** Streamlit Cloud, tracking the repo's default branch.
- **Data:** committed to this repo under `data/partitions/` (see [Data flow](#data-flow)).

---

## Architecture at a glance

```
                ┌─────────────────────────┐
                │  Streamlit Cloud worker │
                │  app.py + analytics/    │
                │       + pre_analytics/  │
                └────────────┬────────────┘
                             │  reads partitions via GitHub Contents API
                             ▼
   ┌─────────────────────────────────────────────────┐
   │  GitHub repo (this one)                         │
   │  data/partitions/YYYY-MM.parquet  ← the "DB"    │
   │  data/partition_index.json        ← table of contents
   │  data/forecasts_*.pkl             ← cached Prophet models
   └────────────────────▲────────────────────────────┘
                        │ writes
       ┌────────────────┴────────────────┐
       │                                 │
┌──────┴────────────┐         ┌──────────┴──────────────┐
│ GitHub Actions    │         │ Mac launchd (06:15 PT)  │
│ xls-ingest.yml    │         │ scripts/email_ingest.py │
│ Power Automate    │         │ Apple Mail → parquet    │
│ → repository      │         │ (standalone, no         │
│   _dispatch       │         │  Streamlit dep)         │
└───────────────────┘         └─────────────────────────┘
```

Both ingest paths write directly to the repo's default branch via the
GitHub Contents API. Streamlit reads the same partitions back over
HTTPS on each cache miss.

---

## Quickstart (local dev)

Requires Python 3.11.

```bash
git clone <repo>
cd lab_dashboard_testing
python -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# Provide secrets locally:
mkdir -p ~/.streamlit
cat > ~/.streamlit/secrets.toml <<'EOF'
app_password = "..."
admin_password = "..."

[github]
token = "..."  # PAT with contents:write on this repo
repo  = "owner/lab_dashboard_testing"
EOF

streamlit run app.py
```

The app boots at <http://localhost:8501>. Without the `[github]` block
it falls back to local-file upload mode (no partition storage).

---

## How to make common changes

These are the things you'll actually want to change. All live in
`config.py` unless noted.

### Add a new bench / lab site

Add one entry to `SITE_CONFIG`:

```python
SITE_CONFIG["My New Bench"] = {
    "resources": ["RESOURCE_ONE", "RESOURCE_TWO"],   # service-resource strings
    "vmax": 30,                                       # heatmap colour-scale ceiling
    "short_label": "MyBench",                         # what the radio shows
    "tat_targets": None,                              # None = use DEFAULT_TAT_TARGETS
    "use_core_panel_defaults": True,                  # True = TAT view defaults to CBC/CMP/BMP/...
}
```

Every downstream consumer (analytics radio, TAT picker, resource lists,
forecast retraining) reads from `SITE_CONFIG`. No other files to edit.

### Add or rename a procedure display alias

Edit `procedure_aliases.py:PROCEDURE_DISPLAY_ALIASES`. **Aliases are
display-only — they are applied at read time inside
`storage.load_filtered_data`, never at ingest.** Partition data on disk
keeps the verbose canonical names ("Comprehensive Metabolic Panel"),
so adding or changing an alias takes effect immediately for all
historical data with no migration.

If a new procedure name from the source data arrives with a `\xa0`
non-breaking-space variant that needs collapsing, add a tuple to
`PROCEDURE_WHITESPACE_NORMALIZATIONS` in the same file (whitespace
normalisation IS applied at ingest because it's data cleanup, not
display).

### Adjust a TAT service-level target

`config.py:DEFAULT_TAT_TARGETS` for the global default (RT / ST / TS in
minutes). For per-bench overrides, set `tat_targets` on that bench's
`SITE_CONFIG` entry (e.g. Norris Specialty uses a 48-hour flat SLA
for send-outs).

### Add a resource remap (consolidate two service resources)

`config.py:RESOURCE_REMAPS`. Keyed by `(procedure, old_resource) →
new_resource`. Applied at read time, so it takes effect for all
historical data immediately.

### Change which procedures are excluded from analytics

`config.py:EXCLUDED_PROCEDURES` — set of order-procedure strings to
drop. Affects all three time bases (Completed / In-Lab / TAT).

---

## Data flow

1. **Source:** the EHR emails a daily XLS export ("Lab Order Department
   Volume Analysis – All Labs Daily Report") to the lab inbox.
2. **Ingest** (one of two paths):
   - **Primary — Power Automate → GitHub Actions:** Power Automate
     watches the inbox and fires a `repository_dispatch` event with
     `event_type = "xls-ingest"` and the base64-encoded XLS as
     `client_payload`. `.github/workflows/xls-ingest.yml` runs
     `.github/scripts/ingest_xls.py`, which parses and writes via
     `parsing.parse_single_file()` + `storage.ingest_new_data()`. Uses
     a Streamlit stub so the production code can run without a real
     Streamlit session.
   - **Secondary — Mac launchd:** `scripts/email_ingest.py` runs daily
     at 06:15 local time via `com.usc.lab-dashboard-ingest.plist`.
     Self-contained (inlined parsing + GitHub Contents API writes).
     See `scripts/README.md` for setup.
3. **Storage:** monthly partitions at `data/partitions/YYYY-MM.parquet`,
   each tracked by SHA in `data/partition_index.json`. New ingests
   merge into the existing partition for that month (dedup by
   `Date/Time - Complete` range), then commit the updated partition
   and index in a single push.
4. **Dashboard read:** `storage.load_filtered_data` reads only the
   partitions whose date range overlaps the query, applies whitespace
   normalisation + display aliases + resource remaps, and filters in
   pandas. Per-partition results are cached by SHA, so unchanged
   partitions stay hot across sessions on the same Streamlit worker.

---

## Deployment

- **Streamlit Cloud** tracks the repo's default branch. Push to that
  branch and Streamlit Cloud rebuilds + redeploys (~2 minutes).
- **Secrets** are configured in the Streamlit Cloud dashboard under
  the app's "Secrets" panel — same keys as the local `secrets.toml`
  above.
- **GitHub Actions** picks up workflow changes on push to default
  branch. Power Automate's dispatch URL doesn't change.

---

## Operations cheatsheet

| Problem | Fix |
|---|---|
| Forecast looks stale / out of date | Dashboard → Data Management → **Refresh Forecast** (re-trains Prophet for all benches; takes ~5 min) |
| Today's data didn't show up | Check Actions tab for a failed `xls-ingest` run, OR check `scripts/` launchd log (`tail -f ~/Library/Logs/lab-dashboard-ingest.log`) |
| Bad data on a specific date range | Data Management → **Remove a date range** → re-ingest the source XLS via the upload UI |
| Need to wipe and restart | Data Management → **Reset** (deletes all partitions + index; only use in emergencies) |
| Dashboard hangs / OOM on Streamlit Cloud | Worker likely cached too many partitions; click "Refresh data" in Data Management to clear partition cache |
| Procedure name showing as full canonical name instead of short alias | Add it to `PROCEDURE_DISPLAY_ALIASES` in `procedure_aliases.py`; takes effect on next Streamlit Cloud reload |

---

## Project structure

```
app.py                      Entry point. Bench config, login gate,
                            dashboard dispatcher.
analytics/                  Analytics dashboard (Completed / In-Lab / TAT,
                            Daily / Monthly views, forecasts).
  dashboard.py              Sidebar + view dispatch.
  data.py                   Pivot builders, monthly-avg comparisons.
  views/                    daily.py, monthly.py, tat.py.
pre_analytics/              Phlebotomy draws dashboard.
  dashboard.py              Sidebar + view dispatch.
  data.py                   Draw-data loaders.
  views/                    Heatmap renderers, hourly bar, weekday pattern.
ui/                         Shared UI components.
  css.py                    All custom CSS (1.5K lines, one big string).
  header.py                 Top banner + Analytics/Pre-Analytics toggle.
  cards.py                  KPI cards, status chips.
  data_management.py        Sidebar admin expander.
config.py                   SITE_CONFIG, RESOURCE_REMAPS, TAT targets,
                            datetime columns, LAB_TZ.
procedure_aliases.py        Whitespace normalisation + display aliases.
parsing.py                  XLS / XLSX / CSV → cleaned DataFrame.
storage.py                  Partition reads/writes via GitHub Contents API.
forecasting.py              Prophet training + cached predictions.
scripts/                    Mac launchd ingest path (see scripts/README.md).
.github/
  workflows/xls-ingest.yml  Power Automate → ingest Action.
  scripts/ingest_xls.py     Action's Python runner (installs Streamlit stub).
tests/                      pytest suite. test_pivots.py, test_tat_metrics.py.
data/
  partitions/*.parquet      Monthly partitions. Committed to git.
  partition_index.json      SHA + row count + date range per partition.
  forecasts_*.pkl           Cached Prophet models per bench.
```

---

## Secrets

The app reads these from `st.secrets`:

| Key | Purpose |
|---|---|
| `app_password` | Login gate. Required to see any dashboard. |
| `admin_password` | Gates the Data Management expander's admin body. |
| `github.token` | PAT with `contents:write` for the data repo. |
| `github.repo` | `owner/name` of the data repo. |

The GitHub Action receives `GITHUB_TOKEN` + `GITHUB_REPOSITORY` from
Actions' built-in secrets — no manual setup.

---

## Tests

```bash
pytest tests/ -q
```

68 tests covering pivot builders and TAT metrics. No integration tests
for the ingest pipeline — the closest substitute is to run
`.github/scripts/ingest_xls.py` locally with a real payload (see the
file's docstring for env-var setup).

---

## Gotchas worth knowing

- **Partition data is committed to git.** `data/partitions/*.parquet` is
  the database. Don't "clean up" the repo by deleting them.
- **Storage writes target the default branch.** `storage.py` calls the
  GitHub Contents API without a `ref=` parameter, so ingests always
  land on whatever GitHub considers the default branch. Renaming the
  default branch retargets ingest automatically; pushing partial
  ingests through feature branches doesn't work.
- **Tz handling.** `parsing._localize_lab_datetimes` localises raw
  datetimes to `LAB_TZ` (America/Los_Angeles) at ingest. The read path
  re-localises tz-naive partitions defensively so mixed-vintage data
  concats cleanly. DST nonexistent / ambiguous times become `NaT`.
- **Streamlit Cloud RAM ceiling is 1 GB.** The partition-streaming read
  path was built specifically for this — don't introduce a full
  `pd.concat` over every partition in any new code.
- **Two ingest paths must stay in sync (whitespace rules only).**
  `parsing.py` and `scripts/email_ingest.py` duplicate the
  whitespace-normalisation rules intentionally — the Mac script runs
  standalone with no repo-internal imports. If you change the rules
  in one place, mirror in the other. Display aliases stay
  read-side-only and don't need mirroring.
