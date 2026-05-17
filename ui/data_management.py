"""
ui/data_management.py — Sidebar Data Management expander.

Shared between analytics and pre-analytics: lets the admin user
authenticate, see partition stats, trigger an upload / delete /
reset / forecast retrain, and surface the latest ingest stats.
"""

import pandas as pd
import streamlit as st

from ui.cards import status_chip


# ═════════════════════════════════════════════════════════════════════════════
# DATA MANAGEMENT SIDEBAR (shared between analytics + pre-analytics)
# ═════════════════════════════════════════════════════════════════════════════

def render_data_management_sidebar(
    ss,
    *,
    data_exists: bool,
    data_summary: dict,
    load_err: Exception | None,
) -> None:
    """Render the Data Management expander inside `with st.sidebar:`.

    Shared between analytics and pre-analytics dashboards. Both
    dashboards call this function from their sidebar render path
    after computing `data_exists`, `data_summary`, `load_err` (which
    they already do via `storage.get_data_summary()` / similar). Only
    handles the `storage_is_configured()` path; the local-file-upload
    fallback stays inline in analytics/dashboard.py (pre-analytics
    doesn't use that fallback).

    Body order:
      1. Status chip + storage caption + divider.
      2. Admin password gate.
      3. Admin-only body: refresh / refresh forecast / current
         dataset summary / remove-a-date-range / file upload / reset.

    The previous Export Raw Data button was REMOVED because
    `st.download_button` evaluates its `data=` argument at script-
    define time (not click time). For a multi-month parquet dataset
    this meant loading 4M+ rows + xlsx-serializing on every script
    rerun, OOMing the Streamlit Cloud worker.
    """
    # Late imports — avoid module-level import of storage (which
    # initializes DuckDB connections etc.) for tests / dry-runs.
    from storage import count_rows_in_date_range

    st.markdown("---")
    with st.expander("Data Management", expanded=not data_exists):
        # Status chip — error / data summary / no-data warn.
        if load_err is not None:
            status_chip("Load error", level="error")
            st.error(f"Could not read data index: {load_err}")
        elif data_exists:
            status_chip(
                f"{data_summary['total_rows']:,} rows · "
                f"{data_summary['min_date']} → {data_summary['max_date']}",
                level="ok",
            )
        else:
            status_chip("No data yet - upload below", level="warn")
        st.caption("Storage: GitHub (partitioned)")
        st.markdown("---")

        # ── Admin password gate (existing behaviour). ───────────────
        admin_pw   = st.secrets.get("admin_password", None)
        authorized = True
        if admin_pw:
            if ss.get("admin_authorized", False):
                authorized = True
            else:
                entered_pw = st.text_input(
                    "Admin password", type="password", key="admin_pw"
                )
                if entered_pw == admin_pw:
                    ss["admin_authorized"] = True
                    st.rerun()
                elif entered_pw:
                    st.error("Incorrect password.")
                authorized = ss.get("admin_authorized", False)

        if not authorized:
            return

        # ── Admin-only body. ────────────────────────────────────────
        st.markdown('<div class="refresh-btn">', unsafe_allow_html=True)
        if st.button("↺  Refresh data", width="stretch", key="refresh_data_btn"):
            st.cache_data.clear()
            ss.pop("_partition_index", None)
            st.rerun()
        st.markdown('</div>', unsafe_allow_html=True)

        if st.button(
            "⟳  Refresh Forecast", width="stretch",
            key="refresh_forecast_btn",
            disabled=(not data_exists),
        ):
            ss["pending_forecast_retrain"] = True
            st.rerun()

        if data_exists:
            st.markdown("**Current dataset**")
            st.caption(
                f"Rows: **{data_summary['total_rows']:,}**  \n"
                f"Date range: **{data_summary['min_date']}** → "
                f"**{data_summary['max_date']}**  \n"
                f"Partitions: **{data_summary['partitions']}**"
            )

            with st.expander("Remove a date range", expanded=False):
                st.caption(
                    "Permanently deletes all rows in the chosen window "
                    "from the master dataset.  This cannot be undone."
                )
                _dr_min = date.fromisoformat(data_summary["min_date"])
                _dr_max = date.fromisoformat(data_summary["max_date"])
                _dc1, _dc2 = st.columns(2)
                with _dc1:
                    del_start = st.date_input(
                        "From", value=_dr_min,
                        min_value=_dr_min, max_value=_dr_max,
                        key="del_start",
                    )
                with _dc2:
                    del_end = st.date_input(
                        "To", value=_dr_min,
                        min_value=_dr_min, max_value=_dr_max,
                        key="del_end",
                    )
                if del_start > del_end:
                    st.error("'From' date must be on or before 'To' date.")
                else:
                    _affected = count_rows_in_date_range(del_start, del_end)
                    if _affected:
                        st.warning(f"Will delete **{_affected:,}** rows.")

                if st.button(
                    "Delete this range", type="primary",
                    width="stretch", key="btn_del_range",
                    disabled=(del_start > del_end),
                ):
                    ss["pending_delete_range"] = {
                        "start": del_start, "end": del_end
                    }
                    st.rerun()

            st.markdown("---")

        st.markdown("**Step 1 - Select file(s)**")
        new_files = st.file_uploader(
            "Upload files", type=["xlsx", "xls", "csv"],
            key="admin_upload",
            label_visibility="collapsed",
            accept_multiple_files=True,
        )
        if new_files:
            _staged = []
            for _uf in new_files:
                _staged.append({"bytes": _uf.read(), "name": _uf.name})
            ss["staged_files"] = _staged
            _names = ", ".join(f"**{s['name']}**" for s in _staged)
            st.caption(f"Ready: {_names}  ({len(_staged)} file(s))")

            st.markdown("**Step 2 - Add to master dataset**")
            if st.button(
                "Process & add to master",
                type="primary", width="stretch",
                key="btn_process_upload",
            ):
                ss["pending_upload"] = ss.pop("staged_files")
                st.rerun()
        else:
            # User cleared the uploader (or never picked a file). Drop
            # any bytes previously staged — otherwise 10s of MB of
            # uploaded XLSX content sit in session_state indefinitely
            # whenever someone uploads and then changes their mind.
            ss.pop("staged_files", None)

        st.markdown("---")
        st.markdown("**Danger zone**")
        if st.button(
            "Reset - delete all data", width="stretch",
            key="btn_reset_all",
        ):
            ss["pending_reset"] = True
            st.rerun()
