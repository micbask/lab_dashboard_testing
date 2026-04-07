"""
ingest_xls.py — GitHub Actions runner glue for repository_dispatch ingests.

Reads a JSON payload from $PAYLOAD_PATH containing:
    { "filename": "...", "file_b64": "<base64-encoded file bytes>" }

Calls parsing.parse_single_file() to build the cleaned DataFrame, then
calls storage.ingest_new_data() to write it into the partitioned Parquet
store on GitHub — exactly the same path the Streamlit app uses.

storage.py depends on `streamlit` (st.secrets / st.session_state /
st.cache_data). Rather than refactor storage.py, we install a minimal
stub `streamlit` module into sys.modules BEFORE importing storage, so
all the existing partition-write logic runs unchanged on the Actions
runner.

Required environment variables:
    GITHUB_TOKEN       — token with contents:write on this repo
    GITHUB_REPOSITORY  — "owner/name" (provided automatically by Actions)
    PAYLOAD_PATH       — path to the JSON payload file
"""

import base64
import json
import os
import sys
import types


# ──────────────────────────────────────────────────────────────────────────
# Streamlit stub — must be installed BEFORE importing storage.
# ──────────────────────────────────────────────────────────────────────────
def _install_streamlit_stub() -> None:
    st = types.ModuleType("streamlit")

    st.secrets = {
        "github": {
            "token": os.environ["GITHUB_TOKEN"],
            "repo": os.environ["GITHUB_REPOSITORY"],
        }
    }
    st.session_state = {}

    def _noop_cache(*dargs, **dkwargs):
        # Supports both `@st.cache_data` and `@st.cache_data(ttl=300, ...)`
        if dargs and callable(dargs[0]) and not dkwargs:
            return dargs[0]

        def _decorator(fn):
            return fn

        return _decorator

    st.cache_data = _noop_cache
    st.cache_resource = _noop_cache

    # A handful of no-op UI calls so any incidental st.write / st.warning
    # inside imported modules don't blow up at runtime.
    def _noop(*args, **kwargs):
        return None

    for _name in ("write", "warning", "info", "error", "success", "stop"):
        setattr(st, _name, _noop)

    sys.modules["streamlit"] = st


def main() -> int:
    _install_streamlit_stub()

    # Imported AFTER the stub is installed.
    from parsing import parse_single_file  # noqa: E402
    from storage import ingest_new_data  # noqa: E402

    payload_path = os.environ["PAYLOAD_PATH"]
    with open(payload_path, "r", encoding="utf-8") as f:
        payload = json.load(f)

    filename = payload.get("filename") or "upload.xls"
    file_b64 = payload.get("file_b64")
    if not file_b64:
        print("::error::Payload is missing 'file_b64'", file=sys.stderr)
        return 2

    try:
        file_bytes = base64.b64decode(file_b64, validate=True)
    except Exception as exc:
        print(f"::error::Failed to base64-decode file_b64: {exc}", file=sys.stderr)
        return 2

    print(f"Received file: {filename} ({len(file_bytes):,} bytes)")

    df = parse_single_file(file_bytes, filename=filename)
    if df is None or df.empty:
        print("::warning::parse_single_file produced 0 rows — nothing to ingest")
        return 0

    print(f"Parsed {len(df):,} rows. Writing to partitioned Parquet store...")
    summary = ingest_new_data(df)
    print(f"Ingest summary: {summary}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
