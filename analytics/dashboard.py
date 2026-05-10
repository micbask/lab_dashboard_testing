import calendar as _cal
from copy import deepcopy
from datetime import date, timedelta

import altair as alt
import numpy as np
import pandas as pd
import plotly.graph_objects as go
import streamlit as st

from config import (
    DEFAULT_RESOURCES, VMAX, ALL_RESOURCES, MAP_TYPES,
    EXCLUDE_PROCS, HOUR_LABELS, LABEL_TO_HOUR,
)
from storage import (
    storage_is_configured, get_data_summary,
    load_filtered_data,
    delete_date_range, reset_all_data,
    ensure_partitioned_storage, get_index_hash,
    count_rows_in_date_range,
)
from forecasting import (
    load_forecasts, retrain_all_forecasts_streaming,
    build_forecast_pivot,
)
from parsing import parse_single_file, deduplicate_and_merge, clean_procedure_names
from ui_components import (
    metric_card, render_header, status_chip,
    style_monthly_pivot,
    build_png, build_monthly_png, build_weekday_png,
)
from analytics.data import (
    build_pivot, build_monthly_pivot, build_weekday_pivot,
    load_monthly_avg_for_comparison,
)


# Viridis_r / Oranges endpoint hex codes for the small legend swatches
# that accompany each Plotly heatmap. Picked off Plotly's built-in
# colorscales so the swatch matches the actual gradient in the chart.
# Note Viridis is REVERSED on the analytics heatmaps (Viridis_r) so dark
# = high values, yellow = low values.
_VIRIDIS_LOW  = "#fde725"   # bright yellow — Viridis_r low end
_VIRIDIS_HIGH = "#440154"   # deep purple   — Viridis_r high end
_ORANGES_LOW  = "#fff5eb"
_ORANGES_HIGH = "#7f2704"

# Solid neutral fill used to render the Total column on every analytics
# heatmap. Keeps Total cells out of the colorscale so a wide-range
# full-day sum doesn't compress the per-hour gradient.
_TOTAL_NEUTRAL = "#ececec"


def _build_analytics_heatmap(
    pivot: pd.DataFrame,
    *,
    colorscale: str,
    hovertemplate: str,
    customdata=None,
) -> go.Figure:
    """Render the analytics dashboard heatmap as a Plotly figure.

    Mirrors the pre_analytics conventions: integer text labels in cells,
    `xgap/ygap=1`, no colorbar (`showscale=False`), `dragmode=False`,
    locked axis ranges, transparent background, and a white hover tooltip
    matching the rest of the app.

    Two `go.Heatmap` traces share a single numeric x-axis so the chart
    renders as one continuous heatmap:

      • Hour columns (24 cells) — coloured with `colorscale`. `zmax`
        is the 95th percentile of non-zero hour cells, so wide-range
        full-day totals never get to compress the per-hour gradient.
        Hour cells above zmax clip to the high gradient colour
        (Plotly's default zmax behaviour).
      • Total column (1 cell)   — rendered with a flat neutral grey
        via a 2-stop single-colour colorscale, completely independent
        of the hour gradient.

    Numeric x-coords (0..23 for hours, 24 for Total) are used because
    using categorical x labels for two side-by-side traces caused the
    chart to render at ~half width. Tick labels are restored via
    `tickvals`/`ticktext`.

    `customdata`, when supplied, must match the pivot's full shape
    (n_rows × n_cols). Each cell value is paired with its column label
    so hovertemplates can reference `%{customdata[0]}` (column label)
    and `%{customdata[1]}` (cell value or pre-formatted string). When
    `customdata` is None, the cell values default to the original z.
    """
    z_full = pivot.values.astype(float)
    y = pivot.index.tolist()
    cols = pivot.columns.tolist()

    if "Total" in cols:
        total_idx    = cols.index("Total")
        hour_indices = [i for i in range(len(cols)) if i != total_idx]
        hour_cols    = [cols[i] for i in hour_indices]
        z_hours      = z_full[:, hour_indices]
        z_total      = z_full[:, [total_idx]]
    else:
        total_idx    = None
        hour_indices = list(range(len(cols)))
        hour_cols    = cols
        z_hours      = z_full
        z_total      = None

    # Default per-cell customdata value to the original z when caller
    # didn't supply anything.
    cell_values = z_full if customdata is None else customdata

    # Pair each cell value with its column label. The numeric x-coords
    # mean `%{x}` would show "0", "1", … so the hovertemplate has to
    # pull the label out of customdata instead.
    cd_hours = [
        [[hour_cols[j], cell_values[i][hour_indices[j]]]
         for j in range(len(hour_indices))]
        for i in range(len(y))
    ]
    cd_total = None
    if total_idx is not None:
        cd_total = [
            [["Total", cell_values[i][total_idx]]]
            for i in range(len(y))
        ]

    # zmax keyed off hour cells only — the Total trace uses its own
    # flat colorscale and isn't part of this gradient.
    _nz = z_hours[z_hours > 0]
    zmax_hours = float(np.percentile(_nz, 95)) if _nz.size else 1.0
    zmax_hours = max(zmax_hours, 1.0)

    text_hours = [
        [str(int(round(v))) if v > 0 else "" for v in row]
        for row in z_hours
    ]

    # Numeric x positions: 0..len(hour_cols)-1 for hour cells, the next
    # integer for the Total cell. Both traces sharing a single numeric
    # x-axis lays the cells out as one continuous heatmap.
    x_hours_coords = list(range(len(hour_cols)))
    x_total_coord  = len(hour_cols)

    fig = go.Figure()
    fig.add_trace(
        go.Heatmap(
            z=z_hours,
            x=x_hours_coords,
            y=y,
            text=text_hours,
            texttemplate="%{text}",
            hoverinfo="text",
            colorscale=colorscale,
            zmin=0,
            zmax=zmax_hours,
            xgap=1,
            ygap=1,
            showscale=False,
            hovertemplate=hovertemplate,
            customdata=cd_hours,
        )
    )

    if z_total is not None:
        text_total = [
            [str(int(round(v))) if v > 0 else "" for v in row]
            for row in z_total
        ]
        fig.add_trace(
            go.Heatmap(
                z=z_total,
                x=[x_total_coord],
                y=y,
                text=text_total,
                texttemplate="%{text}",
                hoverinfo="text",
                # Flat 2-stop colorscale: every Total cell renders as
                # the neutral grey regardless of the underlying value.
                colorscale=[[0.0, _TOTAL_NEUTRAL], [1.0, _TOTAL_NEUTRAL]],
                zmin=0,
                zmax=1,
                xgap=1,
                ygap=1,
                showscale=False,
                hovertemplate=hovertemplate,
                customdata=cd_total,
                textfont=dict(color="#1a1a1a", size=11),
            )
        )

    fig.update_traces(hoverongaps=False)

    tick_vals = list(x_hours_coords)
    tick_text = list(hour_cols)
    if z_total is not None:
        tick_vals.append(x_total_coord)
        tick_text.append("Total")

    # Procedure names are long — automargin gives the y-axis whatever it
    # needs to lay them out without clipping.
    plot_h = max(320, len(y) * 28 + 100)
    fig.update_layout(
        height=plot_h,
        margin=dict(l=10, r=10, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        dragmode=False,
        xaxis=dict(
            tickmode="array",
            tickvals=tick_vals,
            ticktext=tick_text,
            tickfont=dict(size=10), side="bottom", fixedrange=True,
        ),
        yaxis=dict(
            tickfont=dict(size=10), autorange="reversed",
            fixedrange=True, automargin=True,
        ),
        hoverlabel=dict(
            bgcolor="white",
            bordercolor="#6F1828",
            font=dict(
                size=12,
                family="Inter, system-ui, sans-serif",
                color="#1a1a1a",
            ),
            align="left",
        ),
    )
    return fig


def _apply_in_lab_basis(filtered_df: pd.DataFrame) -> pd.DataFrame:
    """Re-map `complete_date` / `hour` to the In-Lab columns.

    Returns the rows that have In-Lab timestamps with `complete_date` /
    `hour` overwritten to the In-Lab values, so downstream pivots key off
    In-Lab time. Returns an empty DataFrame if no In-Lab data is present.
    """
    if (
        filtered_df is None or filtered_df.empty
        or "inlab_date" not in filtered_df.columns
        or not filtered_df["inlab_date"].notna().any()
    ):
        return pd.DataFrame()
    out = filtered_df[filtered_df["inlab_date"].notna()].copy()
    out["complete_date"] = out["inlab_date"]
    out["hour"] = out["inlab_hour"].astype(int)
    return out


def render_sidebar(ss) -> dict:
    """Render analytics sidebar widgets. Returns params dict for render()."""
    with st.sidebar:
        st.markdown("### Map Type")
        map_type = st.selectbox("Map type", MAP_TYPES, label_visibility="collapsed")

        if ss.last_map_type != map_type:
            ss.pop("date_picker", None)
            ss.last_map_type = map_type

        st.markdown("### View")
        view_mode = st.radio(
            "View", ["Daily", "Monthly"],
            horizontal=True, label_visibility="collapsed",
        )

        st.markdown("### Time Basis")
        time_basis = st.radio(
            "Time Basis", ["Completed", "In-Lab"],
            horizontal=True, label_visibility="collapsed",
        )

        if ss.pop("pending_forecast_retrain", False):
            if storage_is_configured():
                with st.spinner("Retraining forecast models…"):
                    retrain_all_forecasts_streaming(ss.resource_assignments)
                st.success("Forecast models retrained.")
            else:
                st.warning("No storage configured — cannot retrain forecasts.")

        if ss.pop("pending_reset", False):
            try:
                if storage_is_configured():
                    reset_all_data()
                    st.cache_data.clear()
                    st.success("Master dataset cleared.")
                for _mt in MAP_TYPES:
                    ss.pop(f"forecasts_{_mt}", None)
            except Exception as _rst_err:
                st.error(f"Reset failed: {_rst_err}")

        if "pending_delete_range" in ss:
            del_info = ss.pop("pending_delete_range")
            try:
                result = delete_date_range(del_info["start"], del_info["end"])
                st.cache_data.clear()
                st.success(
                    f"Deleted {result['rows_removed']:,} rows "
                    f"({del_info['start']} → {del_info['end']})."
                )
            except Exception as _dr_err:
                st.error(f"Delete failed: {_dr_err}")

        st.markdown("---")
        st.markdown("### Data Source")

        _data_exists = False
        _data_summary = {"total_rows": 0, "partitions": 0}
        _local_df = pd.DataFrame()

        if storage_is_configured():
            st.caption("Storage: GitHub (partitioned)")

            try:
                _data_exists = ensure_partitioned_storage()
                if _data_exists:
                    _data_summary = get_data_summary()
                    if _data_summary["total_rows"] == 0:
                        _data_exists = False

                if _data_exists:
                    status_chip(
                        f"{_data_summary['total_rows']:,} rows · "
                        f"{_data_summary['min_date']} → {_data_summary['max_date']}",
                        level="ok",
                    )
                else:
                    status_chip("No data yet — upload below", level="warn")

            except Exception as _load_err:
                status_chip("Load error", level="error")
                st.error(f"Could not read data index: {_load_err}")

            st.markdown("---")
            with st.expander("Data Management", expanded=not _data_exists):

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

                if authorized:

                    st.markdown('<div class="refresh-btn">', unsafe_allow_html=True)
                    if st.button("↺  Refresh data", width="stretch", key="refresh_data_btn"):
                        st.cache_data.clear()
                        ss.pop("_partition_index", None)
                        st.rerun()
                    st.markdown('</div>', unsafe_allow_html=True)

                    if st.button(
                        "⟳  Refresh Forecast", width="stretch",
                        key="refresh_forecast_btn",
                        disabled=(not _data_exists),
                    ):
                        ss["pending_forecast_retrain"] = True
                        st.rerun()

                    if _data_exists:
                        st.markdown("**Current dataset**")
                        st.caption(
                            f"Rows: **{_data_summary['total_rows']:,}**  \n"
                            f"Date range: **{_data_summary['min_date']}** → "
                            f"**{_data_summary['max_date']}**  \n"
                            f"Partitions: **{_data_summary['partitions']}**"
                        )

                        with st.expander("Remove a date range", expanded=False):
                            st.caption(
                                "Permanently deletes all rows in the chosen window "
                                "from the master dataset.  This cannot be undone."
                            )
                            _dr_min = date.fromisoformat(_data_summary["min_date"])
                            _dr_max = date.fromisoformat(_data_summary["max_date"])
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

                    st.markdown("**Step 1 — Select file(s)**")
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

                        st.markdown("**Step 2 — Add to master dataset**")
                        if st.button(
                            "Process & add to master",
                            type="primary", width="stretch",
                        ):
                            ss["pending_upload"] = ss.pop("staged_files")
                            st.rerun()

                    st.markdown("---")
                    st.markdown("**Danger zone**")
                    if st.button("Reset — delete all data", width="stretch"):
                        ss["pending_reset"] = True
                        st.rerun()

        else:
            st.markdown("**Data source:** Local file upload")
            st.caption(
                "Configure GitHub in Streamlit secrets to enable persistent storage."
            )
            uploaded_files = st.file_uploader(
                "Upload Excel file(s)", type=["xlsx", "xls", "csv"],
                accept_multiple_files=True,
            )
            if uploaded_files:
                _parsed = []
                for f in uploaded_files:
                    _df = parse_single_file(f.read(), filename=f.name)
                    if not _df.empty:
                        _parsed.append((f.name, _df))
                if _parsed:
                    _local_df, _ = deduplicate_and_merge(_parsed)
                    _data_exists = not _local_df.empty

        st.markdown("---")

        # Defaults for values only set when data exists
        _min_d = date.today()
        _max_d = date.today()
        _current_resources = ()
        _current_excludes = ()
        _idx_hash = ""
        selected_date = date.today()
        selected_year = date.today().year
        selected_month = date.today().month
        hour_range = (0, 23)
        _fc_data = None
        _is_forecast_date = False

        if _data_exists:
            _current_resources = tuple(sorted(ss.resource_assignments[map_type]))
            _current_excludes = tuple(sorted(EXCLUDE_PROCS))
            _idx_hash = get_index_hash() if storage_is_configured() else ""

            if storage_is_configured():
                _min_d = date.fromisoformat(_data_summary["min_date"])
                _max_d = date.fromisoformat(_data_summary["max_date"])
            else:
                _min_d = _local_df["complete_date"].min()
                _max_d = _local_df["complete_date"].max()

            if view_mode == "Daily":
                _fc_data = load_forecasts(map_type)
                forecast_dates: list = []
                _fc_max_d = _max_d
                if _fc_data:
                    _fc_start_d = _max_d + timedelta(days=1)
                    _fc_end_d   = _fc_data["forecast_end"]
                    forecast_dates = [
                        _fc_start_d + timedelta(days=_i)
                        for _i in range((_fc_end_d - _fc_start_d).days + 1)
                    ]
                    _fc_max_d = _fc_end_d

                if "_pending_date" in ss:
                    _pending = ss.pop("_pending_date")
                    if _min_d <= _pending <= _fc_max_d:
                        ss["date_picker"] = _pending

                if (
                    "date_picker" not in ss
                    or ss["date_picker"] < _min_d
                    or ss["date_picker"] > _fc_max_d
                ):
                    ss["date_picker"] = _max_d

                st.markdown("### Date")
                _fc_note = (
                    f"  +  forecast to **{_fc_max_d}**" if forecast_dates else ""
                )
                st.caption(f"{_min_d} → {_max_d}{_fc_note}")

                picked_date = st.date_input(
                    "Select date",
                    min_value=_min_d,
                    max_value=_fc_max_d,
                    label_visibility="collapsed",
                    key="date_picker",
                )
                selected_date = picked_date
                _is_forecast_date = selected_date > _max_d

                _nc1, _nc2 = st.columns(2)
                with _nc1:
                    if st.button(
                        "◄ Prev", width="stretch",
                        disabled=(selected_date <= _min_d),
                    ):
                        ss["_pending_date"] = selected_date - timedelta(days=1)
                        st.rerun()
                with _nc2:
                    if st.button(
                        "Next ►", width="stretch",
                        disabled=(selected_date >= _fc_max_d),
                    ):
                        ss["_pending_date"] = selected_date + timedelta(days=1)
                        st.rerun()

                if _fc_data:
                    _fc_trained_on = _fc_data.get("last_data_date")
                    if _fc_trained_on is not None and _fc_trained_on != _max_d:
                        st.markdown(
                            '<div style="background:#7a3800;color:#FFE0B2;padding:0.5rem 0.7rem;'
                            'border-radius:6px;font-size:0.78rem;margin-top:0.3rem;">'
                            "⚠ Forecast is out of date. Use the <strong>Refresh Forecast</strong> "
                            "button in Data Management to retrain."
                            "</div>",
                            unsafe_allow_html=True,
                        )
                else:
                    st.caption("No forecast available — use Refresh Forecast to generate one.")

                st.markdown("---")
                st.markdown("### Hour Range")

                def _fmt_h(h: int) -> str:
                    hr12 = 12 if h % 12 == 0 else h % 12
                    suf  = "AM" if h < 12 else "PM"
                    return f"{hr12}:00 {suf}"

                hour_range = st.slider(
                    "Hours", 0, 23, (0, 23), label_visibility="collapsed"
                )
                st.caption(f"{_fmt_h(hour_range[0])} → {_fmt_h(hour_range[1])}")

            else:  # Monthly
                _avail_months = []
                d = date(_min_d.year, _min_d.month, 1)
                end_m = date(_max_d.year, _max_d.month, 1)
                while d <= end_m:
                    _avail_months.append((d.year, d.month))
                    if d.month == 12:
                        d = date(d.year + 1, 1, 1)
                    else:
                        d = date(d.year, d.month + 1, 1)

                if not _avail_months:
                    st.warning("No data found for this map type.")
                    st.stop()

                _month_labels = [
                    f"{_cal.month_name[m]} {y}" for y, m in _avail_months
                ]

                st.markdown("### Month")
                _sel_month_label = st.selectbox(
                    "Select month",
                    _month_labels,
                    index=len(_avail_months) - 1,
                    label_visibility="collapsed",
                )
                _sel_idx = _month_labels.index(_sel_month_label)
                selected_year, selected_month = _avail_months[_sel_idx]

            st.markdown("---")
            with st.expander("Resource Allocation", expanded=False):
                st.markdown(
                    "Reassign instruments between maps. "
                    "Each resource should appear in only one map."
                )
                new_assignments = {}
                for mt in MAP_TYPES:
                    new_assignments[mt] = st.multiselect(
                        mt, options=ALL_RESOURCES,
                        default=ss.resource_assignments.get(mt, []),
                        key=f"res_{mt}",
                    )
                _flat  = [r for rs in new_assignments.values() for r in rs]
                _dupes = sorted({r for r in _flat if _flat.count(r) > 1})
                if _dupes:
                    st.warning(f"Duplicate assignments: {', '.join(_dupes)}")

                _ra, _rb = st.columns(2)
                with _ra:
                    if st.button("Apply", width="stretch", type="primary"):
                        ss.resource_assignments = new_assignments
                        st.cache_data.clear()
                        st.rerun()
                with _rb:
                    if st.button("Reset defaults", width="stretch"):
                        ss.resource_assignments = deepcopy(DEFAULT_RESOURCES)
                        st.cache_data.clear()
                        st.rerun()

    return {
        "map_type": map_type,
        "view_mode": view_mode,
        "time_basis": time_basis,
        "data_exists": _data_exists,
        "data_summary": _data_summary,
        "local_df": _local_df,
        "min_d": _min_d,
        "max_d": _max_d,
        "current_resources": _current_resources,
        "current_excludes": _current_excludes,
        "idx_hash": _idx_hash,
        "selected_date": selected_date,
        "selected_year": selected_year,
        "selected_month": selected_month,
        "hour_range": hour_range,
        "fc_data": _fc_data,
        "is_forecast_date": _is_forecast_date,
    }


def render(params: dict, ss) -> None:
    """Render analytics main panel (pending upload + heatmaps)."""
    map_type          = params["map_type"]
    view_mode         = params["view_mode"]
    time_basis        = params["time_basis"]
    _data_exists      = params["data_exists"]
    _local_df         = params["local_df"]
    _max_d            = params["max_d"]
    _current_resources = params["current_resources"]
    _current_excludes  = params["current_excludes"]
    _idx_hash          = params["idx_hash"]

    # ── Pending upload ─────────────────────────────────────────────────────
    if ss.get("pending_upload"):
        upload_list = ss["pending_upload"]
        if isinstance(upload_list, dict):
            upload_list = [upload_list]

        _n_files   = len(upload_list)
        _file_names = ", ".join(f['name'] for f in upload_list)
        render_header(map_type, "Processing upload…")

        with st.status(f"Processing {_n_files} file(s)…", expanded=True) as _upload_status:
            try:
                st.write(f"**Step 1 / 3** — Parsing {_n_files} file(s): {_file_names}")
                _parsed_frames = []
                for _uf_info in upload_list:
                    _uf_df = parse_single_file(_uf_info["bytes"], filename=_uf_info["name"])
                    st.write(
                        f"  • `{_uf_info['name']}`: **{len(_uf_df):,}** rows "
                        f"({_uf_df['complete_date'].min()} → {_uf_df['complete_date'].max()})"
                    )
                    _parsed_frames.append((_uf_info["name"], _uf_df))
                if len(_parsed_frames) == 1:
                    new_df = _parsed_frames[0][1]
                else:
                    new_df, _ = deduplicate_and_merge(_parsed_frames)
                st.write(f"Combined: **{len(new_df):,}** rows from {_n_files} file(s)")

                st.write("**Step 2 / 3** — Cleaning and validating…")
                _up_bad = int(
                    new_df["Order Procedure"]
                    .str.contains("\xa0", regex=False, na=False)
                    .sum()
                )
                if _up_bad > 0:
                    new_df = clean_procedure_names(new_df)
                    st.write(f"Procedure names corrected ({_up_bad:,} row(s) fixed).")

                st.write("**Step 3 / 3** — Ingesting into partitioned storage…")
                from storage import ingest_new_data
                stats = ingest_new_data(new_df)
                st.write(
                    f"Storage updated: **{stats['rows_before']:,}** → "
                    f"**{stats['rows_after']:,}** rows (+{stats['rows_added']:,} new)"
                )

                ss.pop("pending_upload", None)
                st.cache_data.clear()
                ss.pop("_partition_index", None)
                _upload_status.update(
                    label=f"Done — added {stats['rows_added']:,} rows.",
                    state="complete",
                )
                st.rerun()

            except Exception as _proc_err:
                _upload_status.update(
                    label="Processing failed — existing data is unchanged.", state="error"
                )
                st.error(str(_proc_err))
                ss.pop("pending_upload", None)

        st.stop()

    # ── No-data guard ──────────────────────────────────────────────────────
    if not _data_exists:
        render_header(map_type, "—")
        st.info(
            "Welcome!  Upload a file or configure GitHub in your "
            "Streamlit secrets to start viewing lab productivity heatmaps."
        )
        st.stop()

    # ── Daily view ─────────────────────────────────────────────────────────
    if view_mode == "Daily":
        selected_date    = params["selected_date"]
        hour_range       = params["hour_range"]
        _fc_data         = params["fc_data"]
        _is_forecast_view = params["is_forecast_date"]

        if _is_forecast_view:
            _fc_panel_data = load_forecasts(map_type)
            if _fc_panel_data is None:
                st.warning(
                    f"No forecast data available for **{map_type}**.  "
                    "Open Data Management and click **Refresh Forecast** to generate predictions."
                )
                st.stop()
            pivot, hours = build_forecast_pivot(
                _fc_panel_data, selected_date, hour_range, time_basis=time_basis
            )
            df_date_hour = None
            df_date      = pd.DataFrame()
        else:
            if storage_is_configured():
                filtered_df = load_filtered_data(
                    start_date=selected_date,
                    end_date=selected_date,
                    resources=_current_resources,
                    exclude_procs=_current_excludes,
                    _index_hash=_idx_hash,
                )
            else:
                from config import EXCLUDE_PROCS as _EP
                resources = ss.resource_assignments[map_type]
                filtered_df = _local_df[
                    _local_df["Performing Service Resource"].isin(resources) &
                    ~_local_df["Order Procedure"].isin(_EP)
                ].copy()

            if time_basis == "In-Lab":
                _has_inlab = (
                    "inlab_date" in filtered_df.columns
                    and filtered_df["inlab_date"].notna().any()
                )
                if _has_inlab:
                    filtered_df = filtered_df[filtered_df["inlab_date"].notna()].copy()
                    filtered_df["complete_date"] = filtered_df["inlab_date"]
                    filtered_df["hour"] = filtered_df["inlab_hour"].astype(int)
                else:
                    st.warning("No 'Date/Time - In Lab' data available.")
                    st.stop()

            pivot, df_date_hour, df_date, hours = build_pivot(
                filtered_df, selected_date, hour_range
            )

        date_str = pd.Timestamp(selected_date).strftime("%B %d, %Y")
        render_header(map_type, date_str + ("  ·  Forecast" if _is_forecast_view else ""))

        if _is_forecast_view:
            st.markdown(
                '<div style="background:#1a1a1a;color:#e0e0e0;border-left:4px solid #FF9800;'
                'border-radius:6px;padding:0.85rem 1rem;font-size:0.82rem;margin-bottom:0.5rem;">'
                "This forecast is generated using Prophet, a forecasting ML model trained on all "
                "available historical data. It learns weekly patterns in procedure "
                "volume by hour of day. Predictions are based on limited training data and should "
                "be treated as estimates only."
                "</div>",
                unsafe_allow_html=True,
            )
            st.info(
                "Viewing **forecast** - these are predicted values, not actual completions. "
                "Use the date picker or ◄ Prev / Next ► to return to historical dates."
            )

        if pivot is None:
            if _is_forecast_view:
                st.warning(
                    f"No forecast predictions available for **{map_type}** on **{date_str}** "
                    f"within the selected hour range."
                )
            else:
                st.warning(
                    f"No data found for **{map_type}** on **{date_str}** "
                    f"within the selected hour range.  Try widening the hour slider."
                )
            st.stop()

        _hour_cols = [c for c in pivot.columns if c != "Total"]
        if not _hour_cols or pivot.empty:
            st.info("No completed procedures found for this site on the selected date.")
            st.stop()

        total_vol  = int(round(pivot["Total"].sum()))
        top_proc   = pivot["Total"].idxmax()
        peak_hour  = pivot[_hour_cols].sum().idxmax()
        num_procs  = len(pivot)
        avg_per_hr = round(total_vol / max(len(_hour_cols), 1), 1)
        _vol_label = "Forecast Volume" if _is_forecast_view else "Total Volume"

        _m1, _m2, _m3, _m4, _m5 = st.columns(5)
        with _m1:
            st.markdown(metric_card(_vol_label, f"{total_vol:,}", accent=True),
                        unsafe_allow_html=True)
        with _m2:
            _tp_disp = top_proc[:28] + "…" if len(top_proc) > 28 else top_proc
            st.markdown(metric_card("Top Procedure", _tp_disp,
                        sub=f"{int(round(pivot.loc[top_proc, 'Total'])):,} total"),
                        unsafe_allow_html=True)
        with _m3:
            st.markdown(metric_card("Peak Hour", peak_hour,
                        sub=f"{int(round(pivot[_hour_cols].sum()[peak_hour]))} "
                            f"{'predicted' if _is_forecast_view else 'completions'}"),
                        unsafe_allow_html=True)
        with _m4:
            st.markdown(metric_card("Procedures", str(num_procs), sub="shown (top 30)"),
                        unsafe_allow_html=True)
        with _m5:
            st.markdown(metric_card("Avg / Hour", str(avg_per_hr),
                        sub=f"across {len(_hour_cols)} hours"),
                        unsafe_allow_html=True)

        st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

        if _is_forecast_view:
            _heading_label = "Forecast Volume by Procedure &amp; Hour"
        elif time_basis == "In-Lab":
            _heading_label = "In-Lab Volume by Procedure &amp; Hour"
        else:
            _heading_label = "Completed Volume by Procedure &amp; Hour"
        st.markdown(f'<div class="section-heading">{_heading_label}</div>',
                    unsafe_allow_html=True)

        if _is_forecast_view:
            st.markdown(
                f'<div class="heatmap-legend">'
                f'Colour scale: &nbsp;'
                f'<strong style="color:{_ORANGES_LOW};">■</strong> low &nbsp;→&nbsp; '
                f'<strong style="color:{_ORANGES_HIGH};">■</strong> high '
                f'(hour columns only). &nbsp;'
                f'<strong>Total</strong> column = forecasted full-day sum per procedure.'
                f'</div>',
                unsafe_allow_html=True,
            )
        else:
            st.markdown(
                f'<div class="heatmap-legend">'
                f'Colour scale: &nbsp;'
                f'<strong style="color:{_VIRIDIS_LOW};">■</strong> low &nbsp;→&nbsp; '
                f'<strong style="color:{_VIRIDIS_HIGH};">■</strong> high '
                f'(hour columns only). &nbsp;'
                f'<strong>Total</strong> column = full-day sum per procedure.'
                f'</div>',
                unsafe_allow_html=True,
            )

        # ── Plotly heatmap (replaces the prior st.dataframe HTML table) ─────
        if _is_forecast_view:
            _fig = _build_analytics_heatmap(
                pivot,
                colorscale="Oranges",
                hovertemplate=(
                    "<b>%{y} @ %{customdata[0]}</b><br>"
                    "Forecast: %{customdata[1]:.1f}<extra></extra>"
                ),
            )
        else:
            # Build a procedure × hour avg-per-day pivot for the SAME month
            # (and time basis) so the hover tooltip can compare today's
            # count against the month average for that cell.
            _month_df = pd.DataFrame()
            try:
                if storage_is_configured():
                    _m_start = date(selected_date.year, selected_date.month, 1)
                    _m_end   = date(
                        selected_date.year, selected_date.month,
                        _cal.monthrange(selected_date.year, selected_date.month)[1],
                    )
                    _month_df = load_filtered_data(
                        start_date=_m_start,
                        end_date=_m_end,
                        resources=_current_resources,
                        exclude_procs=_current_excludes,
                        _index_hash=_idx_hash,
                    )
                else:
                    _month_df = _local_df
                if time_basis == "In-Lab":
                    _month_df = _apply_in_lab_basis(_month_df)
            except Exception:
                _month_df = pd.DataFrame()
            _monthly_avg = load_monthly_avg_for_comparison(
                _month_df, selected_date, pivot.index.tolist(),
            )

            _customdata = []
            for _proc in pivot.index:
                _row = []
                for _col in pivot.columns:
                    _val = int(round(pivot.loc[_proc, _col]))
                    if _col == "Total":
                        _row.append(f"Day total: {_val}")
                    else:
                        _hour_int = LABEL_TO_HOUR.get(_col)
                        if (
                            _hour_int is not None
                            and not _monthly_avg.empty
                            and _proc in _monthly_avg.index
                            and _hour_int in _monthly_avg.columns
                        ):
                            _avg = float(_monthly_avg.loc[_proc, _hour_int])
                            _row.append(f"Today: {_val} | Month avg: {_avg:.1f}")
                        else:
                            _row.append(f"Today: {_val}")
                _customdata.append(_row)

            _fig = _build_analytics_heatmap(
                pivot,
                colorscale="Viridis_r",
                hovertemplate=(
                    "<b>%{y} @ %{customdata[0]}</b><br>"
                    "%{customdata[1]}<extra></extra>"
                ),
                customdata=_customdata,
            )

        st.plotly_chart(
            _fig,
            use_container_width=True,
            key="analytics_daily_heatmap",
            config={
                "staticPlot": False,
                "scrollZoom": False,
                "displayModeBar": False,
            },
        )

        _file_prefix = map_type.replace(" ", "_")
        _date_tag    = pd.Timestamp(selected_date).strftime("%Y-%m-%d")

        if not _is_forecast_view and df_date_hour is not None and not df_date_hour.empty:
            _dl1, _dl2 = st.columns(2)
            with _dl1:
                st.download_button(
                    "Download PNG",
                    data=build_png(df_date_hour, map_type, selected_date, hours),
                    file_name=f"{_file_prefix}_{_date_tag}.png",
                    mime="image/png",
                    width="stretch",
                    key="daily_png_dl",
                )
            with _dl2:
                _cols_for_csv = [c for c in [
                    "Order Procedure", "Performing Service Resource",
                    "Date/Time - Complete", "Complete Volume",
                ] if c in df_date.columns]
                _daily_raw_csv = (
                    df_date[_cols_for_csv]
                    .sort_values("Date/Time - Complete")
                    .reset_index(drop=True)
                    .to_csv(index=False)
                    .encode()
                )
                st.download_button(
                    "Download CSV",
                    data=_daily_raw_csv,
                    file_name=f"{_file_prefix}_raw_{_date_tag}.csv",
                    mime="text/csv",
                    width="stretch",
                    key="daily_csv_dl",
                )

        st.markdown("---")

        with st.expander("Hourly Volume", expanded=False):
            _hourly = pivot[_hour_cols].sum().reset_index()
            _hourly.columns = ["Hour", "Total Volume"]
            _hourly_chart = (
                alt.Chart(_hourly)
                .mark_bar()
                .encode(
                    x=alt.X("Hour:N", sort=list(_hour_cols), title="Hour"),
                    y=alt.Y("Total Volume:Q", title="Total Volume"),
                )
                .properties(height=220)
            )
            st.altair_chart(_hourly_chart, use_container_width=True)

        if not _is_forecast_view:
            with st.expander("Drill into a cell — individual completion events",
                             expanded=False):
                st.markdown(
                    "Select a **procedure** and **hour** to inspect every individual "
                    "completion event recorded in that cell."
                )
                _dd1, _dd2 = st.columns(2)
                with _dd1:
                    sel_proc = st.selectbox("Procedure", pivot.index.tolist(),
                                            key="drill_proc")
                with _dd2:
                    sel_hour_label = st.selectbox("Hour", _hour_cols, key="drill_hour")

                sel_hour_int = LABEL_TO_HOUR[sel_hour_label]
                detail = df_date[
                    (df_date["Order Procedure"] == sel_proc) &
                    (df_date["hour"] == sel_hour_int)
                ].copy().sort_values("Date/Time - Complete")

                _show_cols = {k: v for k, v in {
                    "Date/Time - Complete":        "Completed At",
                    "Performing Service Resource": "Resource",
                    "Complete Volume":             "Volume",
                }.items() if k in detail.columns}

                detail_display = (
                    detail[list(_show_cols.keys())]
                    .rename(columns=_show_cols)
                    .reset_index(drop=True)
                )
                if "Completed At" in detail_display.columns:
                    detail_display["Completed At"] = (
                        pd.to_datetime(detail_display["Completed At"])
                        .dt.strftime("%Y-%m-%d  %H:%M:%S")
                    )

                if detail_display.empty:
                    st.info(
                        f"No completions for **{sel_proc}** during "
                        f"**{sel_hour_label}** on {date_str}."
                    )
                else:
                    _cell_vol = (
                        int(detail_display["Volume"].sum())
                        if "Volume" in detail_display.columns else len(detail_display)
                    )
                    st.markdown(
                        f"**{len(detail_display)} event(s)** &nbsp;·&nbsp; "
                        f"*{sel_proc}* &nbsp;·&nbsp; "
                        f"**{sel_hour_label}** &nbsp;·&nbsp; "
                        f"Total volume: **{_cell_vol}**"
                    )
                    st.dataframe(
                        detail_display, width="stretch",
                        height=min(80 + 35 * len(detail_display), 500),
                    )

    else:  # Monthly view
        selected_year  = params["selected_year"]
        selected_month = params["selected_month"]
        month_name_str = f"{_cal.month_name[selected_month]} {selected_year}"
        render_header(map_type, month_name_str)

        _month_start = date(selected_year, selected_month, 1)
        _month_end   = date(selected_year, selected_month,
                            _cal.monthrange(selected_year, selected_month)[1])

        if storage_is_configured():
            filtered_df = load_filtered_data(
                start_date=_month_start,
                end_date=_month_end,
                resources=_current_resources,
                exclude_procs=_current_excludes,
                _index_hash=_idx_hash,
            )
        else:
            resources = ss.resource_assignments[map_type]
            filtered_df = _local_df[
                _local_df["Performing Service Resource"].isin(resources) &
                ~_local_df["Order Procedure"].isin(EXCLUDE_PROCS)
            ].copy()

        if time_basis == "In-Lab":
            _has_inlab = (
                "inlab_date" in filtered_df.columns
                and filtered_df["inlab_date"].notna().any()
            )
            if _has_inlab:
                filtered_df = filtered_df[filtered_df["inlab_date"].notna()].copy()
                filtered_df["complete_date"] = filtered_df["inlab_date"]
                filtered_df["hour"] = filtered_df["inlab_hour"].astype(int)
            else:
                st.warning("No 'Date/Time - In Lab' data available.")
                st.stop()

        monthly_pivot, n_days, month_raw_df = build_monthly_pivot(
            filtered_df, selected_year, selected_month
        )

        if monthly_pivot is None:
            st.warning(f"No data found for **{map_type}** in **{month_name_str}**.")
            st.stop()

        _m_hour_cols   = [c for c in monthly_pivot.columns if c != "Total"]
        _m_total_vol   = int(round(monthly_pivot["Total"].sum() * n_days))
        _m_top_proc    = monthly_pivot["Total"].idxmax()
        _m_peak_col    = monthly_pivot[_m_hour_cols].sum().idxmax()
        _m_peak_disp   = _m_peak_col.replace("AM", " AM").replace("PM", " PM")
        _m_n_procs     = len(monthly_pivot)
        _m_avg_per_day = round(_m_total_vol / max(n_days, 1))

        _mm1, _mm2, _mm3, _mm4, _mm5 = st.columns(5)
        with _mm1:
            st.markdown(metric_card("Total Volume", f"{_m_total_vol:,}", accent=True),
                        unsafe_allow_html=True)
        with _mm2:
            _mtp_disp = _m_top_proc[:28] + "…" if len(_m_top_proc) > 28 else _m_top_proc
            st.markdown(metric_card("Top Procedure", _mtp_disp,
                        sub=f"highest volume in {month_name_str}"),
                        unsafe_allow_html=True)
        with _mm3:
            st.markdown(metric_card("Peak Hour", _m_peak_disp,
                        sub="highest avg volume"), unsafe_allow_html=True)
        with _mm4:
            st.markdown(metric_card("Procedures Shown", str(_m_n_procs),
                        sub="top 30 by month volume"), unsafe_allow_html=True)
        with _mm5:
            st.markdown(metric_card("Avg / Day", f"{_m_avg_per_day:,}",
                        sub=f"over {n_days} days"), unsafe_allow_html=True)

        st.markdown('<hr class="metrics-divider">', unsafe_allow_html=True)

        st.markdown(
            f'<div class="section-heading">'
            f'{map_type} - Monthly Average | {month_name_str} | N = {n_days} days'
            f'</div>',
            unsafe_allow_html=True,
        )
        st.markdown(
            f'<div class="heatmap-legend">'
            f'Values = avg completed volume per day in hour. '
            f'Colour scale: &nbsp;'
            f'<strong style="color:{_VIRIDIS_LOW};">■</strong> low &nbsp;→&nbsp; '
            f'<strong style="color:{_VIRIDIS_HIGH};">■</strong> high '
            f'(hour columns only). &nbsp;'
            f'<strong>Total</strong> column = avg daily total per procedure.'
            f'</div>',
            unsafe_allow_html=True,
        )

        _m_fig = _build_analytics_heatmap(
            monthly_pivot,
            colorscale="Viridis_r",
            hovertemplate=(
                "<b>%{y} @ %{customdata[0]}</b><br>"
                "Avg per day: %{customdata[1]:.1f}<extra></extra>"
            ),
        )
        st.plotly_chart(
            _m_fig,
            use_container_width=True,
            key="analytics_monthly_heatmap",
            config={
                "staticPlot": False,
                "scrollZoom": False,
                "displayModeBar": False,
            },
        )

        _m_file_prefix = map_type.replace(" ", "_")
        _m_month_label = _cal.month_name[selected_month]
        _m_file_tag    = f"{_m_month_label}_{selected_year}"
        _mdl1, _mdl2   = st.columns(2)
        with _mdl1:
            st.download_button(
                "Download PNG",
                data=build_monthly_png(
                    monthly_pivot, map_type, selected_year, selected_month, n_days
                ),
                file_name=f"{_m_file_prefix}_{_m_file_tag}.png",
                mime="image/png",
                width="stretch",
                key="monthly_png_dl",
            )
        with _mdl2:
            _csv_cols = [c for c in [
                "Order Procedure", "Performing Service Resource",
                "Date/Time - Complete", "Complete Volume",
            ] if c in filtered_df.columns]
            _monthly_raw_csv = (
                filtered_df[_csv_cols]
                .sort_values("Date/Time - Complete")
                .reset_index(drop=True)
                .to_csv(index=False)
                .encode()
            )
            st.download_button(
                "Download CSV",
                data=_monthly_raw_csv,
                file_name=f"{_m_file_prefix}_raw_{_m_file_tag}.csv",
                mime="text/csv",
                width="stretch",
                key="monthly_csv_dl",
            )

        st.markdown("---")

        with st.expander("Hourly Volume by Day of Week", expanded=False):
            weekday_pivot, _wd_counts = build_weekday_pivot(
                month_raw_df, selected_year, selected_month
            )

            if weekday_pivot is None:
                st.info("No data available for weekday breakdown.")
            else:
                _wd_hour_cols    = [c for c in weekday_pivot.columns if c != "Total"]
                _wd_busiest_day  = weekday_pivot["Total"].idxmax()
                _wd_lightest_day = weekday_pivot["Total"].idxmin()
                _wd_peak_hour    = weekday_pivot[_wd_hour_cols].sum().idxmax()
                _wd_peak_disp    = _wd_peak_hour.replace("AM", " AM").replace("PM", " PM")

                _wc1, _wc2, _wc3 = st.columns(3)
                with _wc1:
                    st.markdown(
                        metric_card(
                            "Busiest Day",
                            _wd_busiest_day.split("  ")[0],
                            sub=f"avg {int(round(weekday_pivot.loc[_wd_busiest_day, 'Total']))} vol / day",
                        ),
                        unsafe_allow_html=True,
                    )
                with _wc2:
                    st.markdown(
                        metric_card("Peak Hour", _wd_peak_disp,
                                    sub="highest avg volume across weekdays"),
                        unsafe_allow_html=True,
                    )
                with _wc3:
                    st.markdown(
                        metric_card(
                            "Lightest Day",
                            _wd_lightest_day.split("  ")[0],
                            sub=f"avg {int(round(weekday_pivot.loc[_wd_lightest_day, 'Total']))} vol / day",
                        ),
                        unsafe_allow_html=True,
                    )

                st.markdown(
                    f'<div class="section-heading">'
                    f'{map_type} - Weekday Pattern | {month_name_str}'
                    f'</div>',
                    unsafe_allow_html=True,
                )
                _wd_vmax = max(1, int(weekday_pivot[_wd_hour_cols].values.max()))
                st.markdown(
                    f'<div class="heatmap-legend">'
                    f'Values = avg completed volume per occurrence of that weekday. '
                    f'Colour scale: &nbsp;<strong style="color:#f5e642;">■</strong> low &nbsp;→&nbsp; '
                    f'<strong style="color:#3b0f70;">■</strong> high (≥ {_wd_vmax}). &nbsp;'
                    f'<strong>Total</strong> column = avg daily total for that weekday.'
                    f'</div>',
                    unsafe_allow_html=True,
                )

                st.dataframe(
                    style_monthly_pivot(weekday_pivot, _wd_vmax),
                    width="stretch",
                    height=min(80 + 35 * 7, 400),
                )

                _wdl1, _wdl2 = st.columns(2)
                with _wdl1:
                    st.download_button(
                        "Download PNG",
                        data=build_weekday_png(
                            weekday_pivot, map_type, selected_year, selected_month
                        ),
                        file_name=f"{_m_file_prefix}_Weekday_{_m_file_tag}.png",
                        mime="image/png",
                        width="stretch",
                        key="weekday_png_dl",
                    )
                with _wdl2:
                    st.download_button(
                        "Download CSV",
                        data=weekday_pivot.to_csv(index=True).encode(),
                        file_name=f"{_m_file_prefix}_Weekday_{_m_file_tag}.csv",
                        mime="text/csv",
                        width="stretch",
                        key="weekday_csv_dl",
                    )
