# scripts/ — Scheduled Apple Mail → Parquet ingest

Runs a daily job on a Mac that pulls XLS attachments out of **Apple Mail**
(account: `michael.bask@med.usc.edu`) via AppleScript, parses them with
the repo's existing `parsing.parse_single_file()`, and writes them into
the partitioned Parquet store on GitHub using `storage.ingest_new_data()`
— the same path the Streamlit app uses.

## Files

| File | Purpose |
|---|---|
| `fetch_attachment.applescript` | Tells Apple Mail to find messages in the inbox of `michael.bask@med.usc.edu` received in the last 24h whose subject contains `Lab Order Department Volume Analysis- All Labs Daily Report`, save each `.xls`/`.xlsx` attachment to the OneDrive drop folder, and mark the message as read. |
| `email_ingest.py` | Runs the AppleScript via `osascript`, then parses every file sitting in the drop folder with `parse_single_file()`, writes partitions via `storage.ingest_new_data()`, and moves each processed file into `processed/<YYYY-MM-DD>/`. |
| `.env.example` | Template for credentials — copy to `.env` and fill in. Only GitHub credentials are needed; mail credentials live in Apple Mail itself. |
| `requirements.txt` | No extra Python deps — the script is stdlib-only beyond the repo's main `requirements.txt`. |
| `com.usc.lab-dashboard-ingest.plist` | launchd LaunchAgent scheduling the script daily at 06:15 local time. |

## Drop folder

Both the AppleScript and the Python script share this path:

```
/Users/michaelbask/Library/CloudStorage/OneDrive-KeckMedicineofUSC/Work/Productivity Heat Maps/xls_ingest
```

The AppleScript drops new attachments straight into that folder; the
Python script reads them from there, ingests them, and moves them into
`xls_ingest/processed/<YYYY-MM-DD>/` so the next run does not re-process
them. If the path needs to change, update both `fetch_attachment.applescript`
and the `DEFAULT_DROP_FOLDER` constant in `email_ingest.py`, or set
`DROP_FOLDER` in `scripts/.env` to override the Python side.

> **Note on the path:** the original instructions had `~/Users/michaelbask/...`
> which expands to `/Users/michaelbask/Users/michaelbask/...`. The absolute
> path above is what was actually intended.

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

2. **Create a virtualenv and install deps.** The streamlit stub inside
   `email_ingest.py` means you still need the repo's main requirements
   installed so `storage.py` can resolve `@st.cache_data` at import:
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

3. **Populate `scripts/.env`** from the template:
   ```bash
   cp scripts/.env.example scripts/.env
   chmod 600 scripts/.env
   $EDITOR scripts/.env
   ```
   Required values:
   - `GITHUB_TOKEN` — PAT with `contents: write` on the target repo.
   - `GITHUB_REPOSITORY` — `owner/name`, e.g. `micbask/lab_dashboard_testing`.

   Optional:
   - `DROP_FOLDER` — override the default OneDrive drop folder.
   - `LOG_FILE` — override the log path.

   > `scripts/.env` is already ignored by `scripts/.gitignore` — double-check
   > with `git check-ignore scripts/.env` before the first run.

4. **Make sure Apple Mail is signed in to `michael.bask@med.usc.edu`** and
   can see the inbox. The AppleScript addresses the account by its
   `user name` property, with a fallback to a display-name match.

5. **Run it once manually** to confirm it runs the AppleScript, finds
   attachments, parses them, and writes a partition:
   ```bash
   .venv/bin/python scripts/email_ingest.py
   ```
   The **first** run will trigger a macOS consent prompt:
   *"Python" wants access to control "Mail"*. Click **OK** — without it,
   `osascript` will return an error and no attachments will be saved.

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
- **`osascript` errors like `Not authorized to send Apple events to Mail`**
  — Automation consent was denied or never prompted. Open System Settings
  → Privacy & Security → Automation, find the entry for Python (or
  Terminal, if running manually), and tick **Mail**. If no entry exists,
  run the script manually once to trigger the prompt.
- **AppleScript log says `no Mail account found matching …`** — Apple
  Mail is either not running, not signed in to that account, or the
  account's `user name` property is not the UPN. Open Mail → Settings →
  Accounts and confirm the user name; adjust the `targetUser` constant at
  the top of `fetch_attachment.applescript` if needed.
- **`403 Resource not accessible by integration` from GitHub** — the PAT
  doesn't have `contents: write` on the target repo, or points at the
  wrong repo in `GITHUB_REPOSITORY`.
- **Job never fires at 06:15** — check `launchctl list | grep
  lab-dashboard-ingest` for a non-zero exit status, and
  `/tmp/lab-dashboard-ingest.err.log` for tracebacks. Also make sure
  `pmset -g sched` shows a wake for 06:14.
- **Same attachment ingested twice** — the Python script moves every
  processed file into `xls_ingest/processed/<YYYY-MM-DD>/`. If files are
  piling up in the root drop folder, the move step is failing — check
  OneDrive for write permissions on the folder.
