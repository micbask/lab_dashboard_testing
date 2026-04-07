#!/usr/bin/env python3
"""
email_ingest.py — Scheduled Exchange mailbox → Parquet ingest.

Runs locally on a Mac via launchd (see com.usc.lab-dashboard-ingest.plist).

Pipeline per run:
  1. Load credentials from scripts/.env (never hardcoded)
  2. Connect to an Exchange mailbox with exchangelib
  3. Find UNREAD messages from SENDER_EMAIL whose subject contains
     SUBJECT_KEYWORD and that arrived in the last 24 hours
  4. Download each .xls/.xlsx attachment
  5. Call parsing.parse_single_file() to build the cleaned DataFrame
  6. Call storage.ingest_new_data() to write the partitioned Parquet
     files to GitHub via the Contents API (this IS the commit+push —
     each partition write lands on the configured branch as its own
     commit, same path the Streamlit upload uses)
  7. Mark the email as Read so the next run skips it

All existing code in parsing.py and storage.py is reused unchanged.
storage.py depends on `streamlit`; we install a minimal streamlit stub
into sys.modules BEFORE importing storage, exactly like the
.github/scripts/ingest_xls.py GitHub Actions glue does.

Logs are written to the path specified by LOG_FILE (default:
~/Library/Logs/lab-dashboard-ingest.log) so launchd runs leave a trail.

USAGE:
    python3 scripts/email_ingest.py

ENVIRONMENT (scripts/.env — see .env.example):
    EXCHANGE_EMAIL       — primary SMTP, e.g. you@usc.edu
    EXCHANGE_USERNAME    — full UPN / domain login, usually same as EMAIL
    EXCHANGE_PASSWORD    — account password (or app password)
    EXCHANGE_SERVER      — optional; e.g. outlook.office365.com (else autodiscover)
    SENDER_EMAIL         — sender to filter on
    SUBJECT_KEYWORD      — substring to require in the subject line
    GITHUB_TOKEN         — PAT with contents:write on the target repo
    GITHUB_REPOSITORY    — "owner/name", e.g. micbask/lab_dashboard_testing
    LOG_FILE             — optional log path override
"""

from __future__ import annotations

import logging
import os
import sys
import types
from datetime import datetime, timedelta, timezone
from pathlib import Path


# ──────────────────────────────────────────────────────────────────────────
# PATHS & .env LOADING
# ──────────────────────────────────────────────────────────────────────────
SCRIPT_DIR = Path(__file__).resolve().parent
REPO_ROOT = SCRIPT_DIR.parent
ENV_PATH = SCRIPT_DIR / ".env"


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
# EXCHANGE CLIENT
# ──────────────────────────────────────────────────────────────────────────
def _get_required(name: str) -> str:
    val = os.environ.get(name)
    if not val:
        raise SystemExit(f"Missing required env var {name}. See .env.example.")
    return val


def _connect_exchange():
    """Open an exchangelib Account using Basic Auth credentials from .env.

    Note: Microsoft is deprecating Basic Auth for Exchange Online. If your
    tenant has it disabled, use an app password or migrate to OAuth / MS
    Graph. The user explicitly specified username/password, so this path
    uses exchangelib.Credentials.
    """
    from exchangelib import (
        Account,
        Configuration,
        Credentials,
        DELEGATE,
    )

    email = _get_required("EXCHANGE_EMAIL")
    username = os.environ.get("EXCHANGE_USERNAME") or email
    password = _get_required("EXCHANGE_PASSWORD")
    server = os.environ.get("EXCHANGE_SERVER", "").strip()

    creds = Credentials(username=username, password=password)

    if server:
        config = Configuration(server=server, credentials=creds)
        account = Account(
            primary_smtp_address=email,
            config=config,
            autodiscover=False,
            access_type=DELEGATE,
        )
    else:
        account = Account(
            primary_smtp_address=email,
            credentials=creds,
            autodiscover=True,
            access_type=DELEGATE,
        )
    return account


# ──────────────────────────────────────────────────────────────────────────
# MAIN INGEST LOOP
# ──────────────────────────────────────────────────────────────────────────
_XLS_SUFFIXES = (".xls", ".xlsx", ".csv", ".txt")


def _iter_matching_messages(account):
    """Yield inbox messages from SENDER_EMAIL with SUBJECT_KEYWORD in the
    subject, received in the last 24 hours, and still marked unread.
    """
    from exchangelib import EWSDateTime, EWSTimeZone

    sender = _get_required("SENDER_EMAIL").lower()
    subject_kw = _get_required("SUBJECT_KEYWORD")

    tz = EWSTimeZone("UTC")
    since = EWSDateTime.from_datetime(
        datetime.now(timezone.utc) - timedelta(hours=24)
    ).astimezone(tz)

    qs = (
        account.inbox.filter(
            is_read=False,
            datetime_received__gte=since,
            subject__contains=subject_kw,
        )
        .order_by("-datetime_received")
    )

    for msg in qs:
        try:
            from_addr = (msg.sender.email_address or "").lower() if msg.sender else ""
        except Exception:
            from_addr = ""
        if from_addr == sender:
            yield msg


def _process_message(msg) -> int:
    """Parse every XLS/XLSX/CSV attachment on one message and ingest.

    Returns the number of rows added across all attachments.
    """
    from exchangelib import FileAttachment

    total_rows = 0
    processed_any = False

    for att in getattr(msg, "attachments", []) or []:
        if not isinstance(att, FileAttachment):
            continue
        fname = (att.name or "").strip()
        if not fname.lower().endswith(_XLS_SUFFIXES):
            log.info("  • skipping non-spreadsheet attachment: %s", fname)
            continue

        content = att.content  # bytes
        if not content:
            log.warning("  • attachment %s has empty content", fname)
            continue

        log.info("  • parsing attachment: %s (%d bytes)", fname, len(content))
        df = parse_single_file(content, filename=fname)
        if df is None or df.empty:
            log.warning("  • parse_single_file produced 0 rows for %s", fname)
            continue

        log.info("  • ingesting %d rows into partitioned store...", len(df))
        summary = ingest_new_data(df)
        added = summary.get("rows_added", 0)
        log.info("  • ingest summary: %s", summary)
        total_rows += added
        processed_any = True

    if processed_any:
        # Mark as read so the next run skips this message
        msg.is_read = True
        msg.save(update_fields=["is_read"])
        log.info("  • marked email as read")

    return total_rows


def main() -> int:
    log.info("=" * 64)
    log.info("Starting email ingest run")
    log.info("Repo root: %s", REPO_ROOT)

    try:
        account = _connect_exchange()
    except Exception as exc:
        log.exception("Failed to connect to Exchange: %s", exc)
        return 2

    log.info(
        "Connected to Exchange as %s; searching last 24h for unread "
        "from %s with subject containing %r",
        os.environ.get("EXCHANGE_EMAIL"),
        os.environ.get("SENDER_EMAIL"),
        os.environ.get("SUBJECT_KEYWORD"),
    )

    matched = 0
    total_added = 0
    for msg in _iter_matching_messages(account):
        matched += 1
        log.info(
            "Match #%d: %s — %s (received %s)",
            matched, msg.sender.email_address if msg.sender else "?",
            msg.subject, msg.datetime_received,
        )
        try:
            total_added += _process_message(msg)
        except Exception as exc:
            log.exception("Failed to process message %r: %s", msg.subject, exc)

    if matched == 0:
        log.info("No matching messages found.")
    else:
        log.info(
            "Ingest run complete: %d message(s), %d row(s) added.",
            matched, total_added,
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
