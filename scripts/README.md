# scripts/ — Scheduled email → Parquet ingest

Runs a daily job on a Mac that pulls XLS attachments from an Exchange
mailbox, parses them with the repo's existing `parsing.parse_single_file()`,
and writes them into the partitioned Parquet store on GitHub using
`storage.ingest_new_data()` (same path the Streamlit app uses).

## Files

| File | Purpose |
|---|---|
| `email_ingest.py` | The ingest script. Loads `.env`, connects to Exchange, processes unread messages from the last 24h, writes partitions to GitHub, and marks processed messages as read. |
| `.env.example` | Template for credentials — copy to `.env` and fill in. |
| `requirements.txt` | Extra Python dep for this script (`exchangelib`). |
| `com.usc.lab-dashboard-ingest.plist` | launchd LaunchAgent scheduling the script daily at 06:15. |

## How the script writes to GitHub

`storage.ingest_new_data()` writes each affected monthly partition directly
to the repo via the **GitHub Contents API** — each write is itself a commit
on the configured branch. That means the "commit and push" happens inside
`ingest_new_data()` via the PAT from your `.env`, and you do **not** need
to run `git push` separately. No raw XLS files ever touch the repo; only
the derived `data/partitions/YYYY-MM.parquet` files and the updated
`data/partition_index.json`.

## One-time setup

1. **Clone the repo on the Mac** (if not already):
   ```bash
   git clone https://github.com/micbask/lab_dashboard_testing.git ~/lab_dashboard_testing
   cd ~/lab_dashboard_testing
   ```

2. **Create a virtualenv and install deps** (the script's streamlit stub
   means you still need the streamlit package installed for `storage.py`
   to resolve `@st.cache_data` at import — easier to just install the
   repo's full requirements):
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   pip install -r scripts/requirements.txt
   ```

3. **Populate `scripts/.env`** from the template:
   ```bash
   cp scripts/.env.example scripts/.env
   chmod 600 scripts/.env
   $EDITOR scripts/.env
   ```
   Required values:
   - `EXCHANGE_EMAIL` / `EXCHANGE_USERNAME` / `EXCHANGE_PASSWORD` —
     mailbox credentials. If your tenant has Basic Auth disabled,
     create an app password or switch to OAuth (the script currently
     uses `exchangelib.Credentials`, which is Basic Auth).
   - `EXCHANGE_SERVER` — leave blank to use Autodiscover, or set to
     `outlook.office365.com` for Microsoft 365.
   - `SENDER_EMAIL` — exact sender address to filter on.
   - `SUBJECT_KEYWORD` — substring required in the subject line.
   - `GITHUB_TOKEN` — PAT with `contents: write` on the target repo.
   - `GITHUB_REPOSITORY` — `owner/name`, e.g. `micbask/lab_dashboard_testing`.

   > `scripts/.env` should already be ignored by git — double-check with
   > `git check-ignore scripts/.env` before the first run.

4. **Run it once manually** to confirm it connects, finds mail, parses,
   and writes a partition:
   ```bash
   .venv/bin/python scripts/email_ingest.py
   ```
   Logs go to `~/Library/Logs/lab-dashboard-ingest.log` and stdout.

## Register the launchd job

1. **Edit the two placeholders in the plist** (it won't work unmodified):
   ```bash
   sed -e "s|__PYTHON_BIN__|$HOME/lab_dashboard_testing/.venv/bin/python3|g" \
       -e "s|__REPO_PATH__|$HOME/lab_dashboard_testing|g" \
       scripts/com.usc.lab-dashboard-ingest.plist \
     > ~/Library/LaunchAgents/com.usc.lab-dashboard-ingest.plist
   ```

2. **Load the agent:**
   ```bash
   launchctl unload ~/Library/LaunchAgents/com.usc.lab-dashboard-ingest.plist 2>/dev/null
   launchctl load    ~/Library/LaunchAgents/com.usc.lab-dashboard-ingest.plist
   ```

3. **Verify it is scheduled:**
   ```bash
   launchctl list | grep lab-dashboard-ingest
   ```

4. **Schedule a system wake 60 seconds before the job** so the Mac runs
   the ingest even if it's asleep. launchd itself cannot wake the machine;
   `pmset` can. Run once as admin:
   ```bash
   sudo pmset repeat wakeorpoweron MTWRFSU 06:14:00
   pmset -g sched        # confirm the wake schedule
   ```
   > `wakeorpoweron` will power the Mac on from a full shutdown *only* on
   > desktops; laptops on battery will not wake from a lid-closed
   > shutdown. If the Mac is only ever asleep (lid open or plugged in),
   > `wake` is sufficient — substitute `wake` for `wakeorpoweron`.

5. **Force a test run on demand** (bypasses the schedule):
   ```bash
   launchctl start com.usc.lab-dashboard-ingest
   tail -f ~/Library/Logs/lab-dashboard-ingest.log
   ```

## Troubleshooting

- **`Missing required env var ...`** — `scripts/.env` isn't populated
  or launchd doesn't see it. The script loads `.env` relative to its own
  location (`scripts/.env`), so make sure `WorkingDirectory` in the
  plist points at the repo root and the `.env` file exists there.
- **`401 Unauthorized` from Exchange** — Basic Auth is disabled on the
  tenant. Use an app password, or rewrite the Exchange connect block to
  use OAuth (`exchangelib.OAUTH2` flow with a registered app).
- **`403 Resource not accessible by integration`** from GitHub — the PAT
  doesn't have `contents: write` on the target repo, or points at the
  wrong repo in `GITHUB_REPOSITORY`.
- **Job never fires at 06:15** — check `launchctl list | grep
  lab-dashboard-ingest` for a non-zero exit status, and
  `/tmp/lab-dashboard-ingest.err.log` for tracebacks. Also make sure
  `pmset -g sched` shows a wake for 06:14.
- **Mark-as-read didn't stick** — the script only marks a message read
  after at least one attachment was successfully ingested. Check the
  log for `ingesting ... rows into partitioned store` to confirm.
