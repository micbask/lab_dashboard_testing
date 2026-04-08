#!/usr/bin/env python3
"""
email_ingest.py — Scheduled Apple Mail → Parquet ingest.

Runs locally on a Mac via launchd (see com.usc.lab-dashboard-ingest.plist).

Pipeline per run:
  1. Run scripts/fetch_attachment.applescript, which tells Apple Mail
     to find messages from the last 24 hours in the inbox of
     michael.bask@med.usc.edu whose subject contains
     "Lab Order Department Volume Analysis- All Labs Daily Report",
     save every .xls/.xlsx attachment into the OneDrive drop folder,
     and mark each source message as read.
  2. Scan the drop folder for new .xls / .xlsx files.
  3. For each file: call parsing.parse_single_file() to build the
     cleaned DataFrame, then call storage.ingest_new_data() to write
     the partitioned Parquet files to GitHub via the Contents API
     (this IS the commit + push — each partition write lands on the
     configured branch as its own commit, same path the Streamlit
     upload uses).
  4. Move the processed file into a dated `processed/` subfolder so
     the next run does not re-ingest it.

All existing code in parsing.py and storage.py is reused unchanged.
storage.py depends on `streamlit`; we install a minimal streamlit stub
into sys.modules BEFORE importing storage, exactly like the
.github/scripts/ingest_xls.py GitHub Actions glue does.

Logs are written to the path specified by LOG_FILE (default:
~/Library/Logs/lab-dashboard-ingest.log) so launchd runs leave a trail.

USAGE:
    python3 scripts/email_ingest.py

ENVIRONMENT (scripts/.env — see .env.example):
    GITHUB_TOKEN         — PAT with contents:write on the target repo
    GITHUB_REPOSITORY    — "owner/name", e.g. micbask/lab_dashboard_testing
    DROP_FOLDER          — optional override of the OneDrive drop folder
    LOG_FILE             — optional log path override
"""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import types
from datetime import datetime
from pathlib import Path


# ──────────────────────────────────────────────────────────────────────────
# PATHS & .env LOADING
# ──────────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
ENV_PATH = SCRIPT_DIR / ".env"
APPLESCRIPT_PATH = SCRIPT_DIR / "fetch_attachment.applescript"

# Same path hardcoded inside fetch_attachment.applescript. If you change
# one, change the other (or set DROP_FOLDER in .env to override here).
DEFAULT_DROP_FOLDER = Path(
    "/Users/michaelbask/Library/CloudStorage/"
    "OneDrive-KeckMedicineofUSC/Work/Productivity Heat Maps/xls_ingest"
)


def _load_env_file(path: Path) -> None:
    """Minimal .env loader — avoids a python-dotenv hard dependency.

    Supports KEY=VALUE lines, # comments, and optional surrounding quotes.
    Existing os.environ values are NOT overridden (so launchd-set vars win).
    """
    if not path.exists():
        return
    for raw in path.read_text(encoding="utf-8").splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, val = line.partition("=")
        key = key.strip()
        val = val.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = val


_load_env_file(ENV_PATH)


# ──────────────────────────────────────────────────────────────────────────
# LOGGING
# ──────────────────────────────────────────────────────────────────────────
_default_log = Path.home() / "Library" / "Logs" / "lab-dashboard-ingest.log"
LOG_FILE = Path(os.environ.get("LOG_FILE", str(_default_log)))
LOG_FILE.parent.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[
        logging.FileHandler(LOG_FILE, encoding="utf-8"),
        logging.StreamHandler(sys.stdout),
    ],
)
log = logging.getLogger("email_ingest")


# ──────────────────────────────────────────────────────────────────────────
# STREAMLIT STUB — install BEFORE importing storage.py
# ──────────────────────────────────────────────────────────────────────────
def _install_streamlit_stub() -> None:
    """Stub just enough of streamlit so storage.py imports cleanly.

    storage.py uses:
      • st.secrets['github']['token' | 'repo']
      • st.session_state  (dict-like)
      • @st.cache_data (with/without parens)
    """
    st = types.ModuleType("streamlit")

    try:
        st.secrets = {
            "github": {
                "token": os.environ["GITHUB_TOKEN"],
                "repo": os.environ["GITHUB_REPOSITORY"],
            }
        }
    except KeyError as exc:
        raise SystemExit(
            f"Missing required env var {exc}. Populate scripts/.env "
            "(see .env.example)."
        )

    st.session_state = {}

    def _noop_cache(*dargs, **dkwargs):
        if dargs and callable(dargs[0]) and not dkwargs:
            return dargs[0]

        def _decorator(fn):
            return fn

        return _decorator

    st.cache_data = _noop_cache
    st.cache_resource = _noop_cache

    def _noop(*_a, **_kw):
        return None

    for _name in ("write", "warning", "info", "error", "success", "stop"):
        setattr(st, _name, _noop)

    sys.modules["streamlit"] = st


_install_streamlit_stub()

# Repo root on sys.path so we can import the existing parsing/storage modules.
sys.path.insert(0, str(REPO_ROOT))

from parsing import parse_single_file  # noqa: E402  (after stub + sys.path)
from storage import ingest_new_data  # noqa: E402


# ──────────────────────────────────────────────────────────────────────────
# APPLE MAIL FETCH
# ──────────────────────────────────────────────────────────────────────────
def _run_applescript() -> None:
    """Invoke osascript on fetch_attachment.applescript.

    The AppleScript handles the Mail query, attachment save, and
    mark-as-read. stdout/stderr from osascript are forwarded to our log.
    """
    if not APPLESCRIPT_PATH.exists():
        raise SystemExit(f"AppleScript not found: {APPLESCRIPT_PATH}")

    log.info("Running AppleScript: %s", APPLESCRIPT_PATH)
    try:
        proc = subprocess.run(
            ["/usr/bin/osascript", str(APPLESCRIPT_PATH)],
            capture_output=True,
            text=True,
            timeout=300,
        )
    except subprocess.TimeoutExpired:
        log.error("AppleScript timed out after 300s")
        return
    except FileNotFoundError:
        log.error("/usr/bin/osascript not found — is this running on macOS?")
        return

    # osascript `log` statements are emitted on stderr; `return` goes to stdout.
    for stream_name, stream in (("stdout", proc.stdout), ("stderr", proc.stderr)):
        for line in (stream or "").splitlines():
            line = line.strip()
            if not line:
                continue
            # Strip the AppleScript "LINE: " prefix when present.
            if line.startswith("LINE: "):
                log.info("applescript: %s", line[6:])
            else:
                log.info("applescript[%s]: %s", stream_name, line)

    if proc.returncode != 0:
        log.error("AppleScript exited with status %s", proc.returncode)


# ──────────────────────────────────────────────────────────────────────────
# DROP-FOLDER INGEST
# ──────────────────────────────────────────────────────────────────────────
_XLS_SUFFIXES = (".xls", ".xlsx", ".csv", ".txt")


def _drop_folder() -> Path:
    override = os.environ.get("DROP_FOLDER", "").strip()
    return Path(override) if override else DEFAULT_DROP_FOLDER


def _iter_incoming_files(folder: Path):
    """Yield XLS/XLSX/CSV files currently sitting in the drop folder.

    Sub-folders (e.g. `processed/`) are ignored.
    """
    if not folder.exists():
        log.warning("Drop folder does not exist: %s", folder)
        return
    for entry in sorted(folder.iterdir()):
        if entry.is_file() and entry.suffix.lower() in _XLS_SUFFIXES:
            yield entry


def _process_file(path: Path, processed_dir: Path) -> int:
    """Parse and ingest one file, then move it into processed/.

    Returns the number of rows added.
    """
    try:
        content = path.read_bytes()
    except Exception as exc:
        log.exception("  • failed to read %s: %s", path.name, exc)
        return 0

    if not content:
        log.warning("  • %s is empty, skipping", path.name)
        return 0

    log.info("  • parsing %s (%d bytes)", path.name, len(content))
    df = parse_single_file(content, filename=path.name)
    if df is None or df.empty:
        log.warning("  • parse_single_file produced 0 rows for %s", path.name)
        return 0

    log.info("  • ingesting %d rows into partitioned store...", len(df))
    summary = ingest_new_data(df)
    added = int(summary.get("rows_added", 0))
    log.info("  • ingest summary: %s", summary)

    # Move the file into processed/<YYYY-MM-DD>/ to avoid re-processing.
    day_dir = processed_dir / datetime.now().strftime("%Y-%m-%d")
    day_dir.mkdir(parents=True, exist_ok=True)
    dest = day_dir / path.name
    # Avoid clobber if the same filename repeats within a day.
    if dest.exists():
        stem = dest.stem
        suffix = dest.suffix
        i = 1
        while (day_dir / f"{stem}.{i}{suffix}").exists():
            i += 1
        dest = day_dir / f"{stem}.{i}{suffix}"
    shutil.move(str(path), str(dest))
    log.info("  • moved to %s", dest)

    return added


def main() -> int:
    log.info("=" * 64)
    log.info("Starting email ingest run")
    log.info("Repo root: %s", REPO_ROOT)

    # Step 1: ask Apple Mail to drop today's attachments into the folder.
    _run_applescript()

    # Step 2: ingest whatever is sitting in the drop folder.
    drop = _drop_folder()
    drop.mkdir(parents=True, exist_ok=True)
    processed = drop / "processed"
    log.info("Scanning drop folder: %s", drop)

    matched = 0
    total_added = 0
    for f in _iter_incoming_files(drop):
        matched += 1
        log.info("File #%d: %s", matched, f.name)
        try:
            total_added += _process_file(f, processed)
        except Exception as exc:
            log.exception("Failed to process %s: %s", f.name, exc)

    if matched == 0:
        log.info("No incoming files to process.")
    else:
        log.info(
            "Ingest run complete: %d file(s), %d row(s) added.",
            matched, total_added,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
