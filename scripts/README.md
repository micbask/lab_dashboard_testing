# scripts/ — Standalone Apple Mail → Parquet ingest

A self-contained Mac tool that, once a day, pulls XLS attachments from
Apple Mail and writes them into the lab dashboard's partitioned Parquet
store on GitHub. **Nothing outside this folder is required** — you can
drop `scripts/` anywhere on a Mac and it will work.

## What's in the folder

| File | Purpose |
|---|---|
| `email_ingest.py` | The whole ingest job. Parsing (XLS / XLSX / SpreadsheetML / CSV), partition merging, and GitHub Contents-API writes are **all inlined** in this one file — no imports from a parent repo, no Streamlit. |
| `fetch_attachment.applescript` | Drives Apple Mail: finds messages in `michael.bask@med.usc.edu`'s inbox received in the last 24h whose subject contains `Lab Order Department Volume Analysis- All Labs Daily Report`, saves each `.xls`/`.xlsx` attachment to the drop folder, and marks the message as read. |
| `requirements.txt` | Python deps: `pandas`, `pyarrow`, `openpyxl`, `xlrd`, `requests`. |
| `.env.example` | Template for the two GitHub credentials the script needs. Copy to `.env`. |
| `.env` | **You create this** from `.env.example`. Not committed. |
| `.gitignore` | Keeps `.env` and `*.log` out of source control. |
| `com.usc.lab-dashboard-ingest.plist` | launchd LaunchAgent that runs the script daily at 06:15 local time. |

## Setup (three steps)

### 1. Create `.env`

```bash
cd scripts
cp .env.example .env
chmod 600 .env
$EDITOR .env
```

Fill in two values:

```
GITHUB_TOKEN=ghp_xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx
GITHUB_REPOSITORY=micbask/lab_dashboard_testing
```

The PAT needs `contents: write` on that repo. Optional overrides:
`DROP_FOLDER` (the OneDrive path the AppleScript saves into and the
Python reads from) and `LOG_FILE` (default
`~/Library/Logs/lab-dashboard-ingest.log`).

### 2. Install Python deps

```bash
cd scripts
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

A dedicated venv inside `scripts/` keeps the script self-contained,
but any Python 3.10+ environment with `pandas`, `pyarrow`, `openpyxl`,
`xlrd`, and `requests` installed will work.

Run it manually once to confirm everything is wired up:

```bash
./.venv/bin/python email_ingest.py
```

The **first run** will trigger a macOS consent prompt:
*"Python wants to control Mail"*. Click **OK** — without it, the
AppleScript cannot read the mailbox.

Logs stream to stdout and to `~/Library/Logs/lab-dashboard-ingest.log`.

### 3. Register the launchd job

The plist contains two placeholders — `__PYTHON_BIN__` and
`__REPO_PATH__` — that must point at your Python interpreter and your
`scripts/` folder:

```bash
SCRIPTS_DIR="$(pwd)"       # run this from inside scripts/
PY="$SCRIPTS_DIR/.venv/bin/python3"

sed -e "s|__PYTHON_BIN__|$PY|g" \
    -e "s|__REPO_PATH__|$SCRIPTS_DIR|g" \
    com.usc.lab-dashboard-ingest.plist \
  > ~/Library/LaunchAgents/com.usc.lab-dashboard-ingest.plist

launchctl unload ~/Library/LaunchAgents/com.usc.lab-dashboard-ingest.plist 2>/dev/null
launchctl load    ~/Library/LaunchAgents/com.usc.lab-dashboard-ingest.plist
launchctl list | grep lab-dashboard-ingest      # confirm registration
```

> The plist placeholder is called `__REPO_PATH__` for historical reasons
> — it should point at whatever directory holds `email_ingest.py`.
> Since this version is standalone, that's the `scripts/` folder itself.

**Wake the Mac 60 seconds before the job** (launchd alone cannot wake a
sleeping Mac):

```bash
sudo pmset repeat wakeorpoweron MTWRFSU 06:14:00
pmset -g sched         # verify the wake schedule
```

Trigger a test run on demand:

```bash
launchctl start com.usc.lab-dashboard-ingest
tail -f ~/Library/Logs/lab-dashboard-ingest.log
```

## How it works

1. launchd fires `email_ingest.py` at 06:15.
2. Python shells out to `/usr/bin/osascript fetch_attachment.applescript`,
   which logs into Apple Mail, finds matching messages, saves every
   `.xls`/`.xlsx` attachment into
   `/Users/michaelbask/Library/CloudStorage/OneDrive-KeckMedicineofUSC/Work/Productivity Heat Maps/xls_ingest`,
   and marks those messages read.
3. Python scans the drop folder, parses each new file with the inlined
   parsing pipeline (identical to the dashboard's `parse_single_file`),
   and calls the inlined `ingest_new_data` to merge the rows into the
   partitioned Parquet store on GitHub via the Contents API. Each
   affected month's partition is written as its own commit on the
   configured branch — so there is no separate `git push` step.
4. Each processed file is moved into
   `xls_ingest/processed/<YYYY-MM-DD>/` so the next run does not
   re-ingest it.

## Troubleshooting

- **`Missing GITHUB_TOKEN`** — `scripts/.env` is not populated or
  launchd can't find it. The script loads `.env` from the same
  directory as `email_ingest.py`, so make sure the plist's
  `WorkingDirectory` (or the `__REPO_PATH__` substitution above)
  points at the `scripts/` folder.
- **`Not authorized to send Apple events to Mail`** — Automation
  consent was denied or never prompted. Open System Settings → Privacy
  & Security → Automation, find the Python entry, and tick **Mail**.
- **`no Mail account found matching michael.bask@med.usc.edu`** —
  Apple Mail isn't signed in, or the account's User Name field isn't
  that email. Open Mail → Settings → Accounts and confirm. You can
  also edit `targetUser` at the top of `fetch_attachment.applescript`.
- **`403 Resource not accessible by integration`** — the GitHub PAT
  lacks `contents: write` on the repo in `GITHUB_REPOSITORY`.
- **Job never fires at 06:15** — `launchctl list | grep
  lab-dashboard-ingest` shows exit status; `/tmp/lab-dashboard-ingest.err.log`
  has tracebacks; `pmset -g sched` should show the 06:14 wake.
- **Same file ingested twice** — the move to `processed/` failed.
  Check that OneDrive is not syncing the file back or holding a lock.
