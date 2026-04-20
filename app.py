"""
Inspection DS Quality Control Dashboard  v6.1  — Static Data Mode
=================================================================
Fixes over v6:
  - TypeError: Cannot setitem on a Categorical with a new category (0)
    Root cause: .fillna(0) called on whole DataFrame after merge when
    INSP_APP_FIELD / MONTH_LABEL are Categorical dtype.
    Fix: always fill only the specific numeric column, never the whole frame.
  - Same pattern fixed in compute_pivot (mode 2/5 merge paths).
  - MONTH_LABEL forced to plain string in load_all so merges on it can never
    hit category-mismatch errors downstream.
  - Defensive: chart_quality_score, chart_monthly_trend fillna scoped to
    numeric columns only.
  - Empty-data guard added to chart_top_problematic and chart_quality_score.
  - Month ordering now uses explicit sort key instead of Categorical so it
    works whether MONTH_LABEL is str or Categorical.
"""

import io
import numpy as np
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
import streamlit as st

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Inspection DS QC Dashboard",
    page_icon="🔍",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Constants ─────────────────────────────────────────────────────────────────
MONTH_ORDER = ["Oct 2025", "Nov 2025", "Dec 2025", "Jan 2026", "Feb 2026", "Mar 2026"]

DS_LABELS = {
    0: "0 - Correct",
    1: "1 - Missed (alert)",
    2: "2 - Modified",
    3: "3 - Wrong (alert)",
}
DS_COLORS = {0: "#2ecc71", 1: "#e67e22", 2: "#3498db", 3: "#e74c3c"}

METRIC_LABELS = {
    1: "Absolute Count — COUNT(DISTINCT Appt_ID) for selected DS & field & month",
    2: "% vs Field+Month baseline — numerator / all-DS distinct inspections (same field & month)",
    3: "% vs Pivot-Column Total — numerator / sum of pivot values for that month (selected fields & DS)",
    4: "% vs Field baseline (DS-filtered) — numerator / selected-DS distinct inspections (same field, ALL months)",
    5: "% vs Month baseline (all DS) — numerator / all-DS distinct inspections (same month, ALL fields)",
}

DATA_DIR = "data"


# ── CSS ───────────────────────────────────────────────────────────────────────
def inject_css():
    st.markdown("""
    <style>
    .stApp{background-color:#0f1117;color:#e0e0e0}
    [data-testid="metric-container"]{
        background:#1b1f2e;border-radius:10px;
        padding:14px 18px;border:1px solid #2d3250;
        box-shadow:0 2px 8px rgba(0,0,0,.4)
    }
    [data-testid="stMetricValue"]{font-size:1.4rem!important;color:#7eb8f7!important}
    [data-testid="stSidebar"]{background-color:#12151f}
    h1,h2,h3{color:#7eb8f7}
    .block-container{padding-top:1.5rem}
    hr{border-color:#2d3250!important}
    </style>
    """, unsafe_allow_html=True)


# ── Helpers ───────────────────────────────────────────────────────────────────
def _sort_months(items):
    """Sort a list of month strings by MONTH_ORDER."""
    order_map = {m: i for i, m in enumerate(MONTH_ORDER)}
    return sorted(items, key=lambda x: order_map.get(x, 999))


# ── Data loading ──────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Loading data…", ttl=None)
def load_all():
    """
    Load the 4 tiny pre-aggregated parquet files (~1 MB total).
    Called once per app restart, then served from memory forever (ttl=None).

    IMPORTANT: MONTH_LABEL and INSP_APP_FIELD are stored as plain str,
    NOT Categorical.  This prevents:
        TypeError: Cannot setitem on a Categorical with a new category (0)
    which is raised when .fillna(0) is called on a merged DataFrame that
    still contains a Categorical join-key column.
    Month ordering for display is handled by _sort_months() instead.
    """
    try:
        core       = pd.read_parquet(f"{DATA_DIR}/static_core.parquet")
        total_fm   = pd.read_parquet(f"{DATA_DIR}/static_total_fm.parquet")
        total_m    = pd.read_parquet(f"{DATA_DIR}/static_total_m.parquet")
        row_counts = pd.read_parquet(f"{DATA_DIR}/static_rowcounts.parquet")
    except FileNotFoundError as exc:
        st.error(
            f"**Pre-computed data file not found:** `{exc}`\n\n"
            "Run `python precompute_static.py` locally first, "
            "then commit the `data/static_*.parquet` files to your repo."
        )
        st.stop()

    # Force all join-key columns to plain str to avoid Categorical fillna errors
    for df in (core, total_fm, total_m, row_counts):
        df["MONTH_LABEL"] = df["MONTH_LABEL"].astype(str)

    core["INSP_APP_FIELD"]     = core["INSP_APP_FIELD"].astype(str)
    total_fm["INSP_APP_FIELD"] = total_fm["INSP_APP_FIELD"].astype(str)
    core["DS_OUTPUT"]          = core["DS_OUTPUT"].astype(int)
    row_counts["DS_OUTPUT"]    = row_counts["DS_OUTPUT"].astype(int)

    all_fields = sorted(core["INSP_APP_FIELD"].unique().tolist())
    return core, total_fm, total_m, row_counts, all_fields


# ── KPI computation ───────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=None)
def compute_kpis(_row_counts, _total_m, sel_months_t):
    sel_months = list(sel_months_t)
    rc         = _row_counts[_row_counts["MONTH_LABEL"].isin(sel_months)]
    kpi_rows   = rc.groupby("DS_OUTPUT")["row_cnt"].sum().to_dict()
    total_insp = int(_total_m[_total_m["MONTH_LABEL"].isin(sel_months)]["total_m"].sum())
    return kpi_rows, total_insp


# ── Pivot computation ─────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=None)
def compute_pivot(
    _core, _total_fm, _total_m,
    sel_months_t, sel_fields_t, ds_filter_t, metric_mode,
):
    """
    All *_t args are tuples so Streamlit can hash them for caching.
    fillna is always called on a *specific numeric column* — never on the
    whole DataFrame — to avoid the Categorical dtype TypeError.
    """
    sel_months = list(sel_months_t)
    sel_fields = list(sel_fields_t)
    ds_filter  = list(ds_filter_t)

    filtered = _core[
        _core["MONTH_LABEL"].isin(sel_months) &
        _core["INSP_APP_FIELD"].isin(sel_fields) &
        _core["DS_OUTPUT"].isin(ds_filter)
    ].copy()

    if filtered.empty:
        return pd.DataFrame()

    base = (
        filtered
        .groupby(["INSP_APP_FIELD", "MONTH_LABEL"])["n"]
        .sum()
        .reset_index(name="filt_n")
    )

    if metric_mode == 1:
        base["value"] = base["filt_n"]

    elif metric_mode == 2:
        base = base.merge(_total_fm, on=["INSP_APP_FIELD", "MONTH_LABEL"], how="left")
        base["total_fm"] = base["total_fm"].fillna(0)          # numeric only ✓
        base["value"] = np.where(
            base["total_fm"] > 0, base["filt_n"] / base["total_fm"] * 100, 0)

    elif metric_mode == 3:
        col_tot = (base.groupby("MONTH_LABEL")["filt_n"]
                   .sum().reset_index(name="col_total"))
        base = base.merge(col_tot, on="MONTH_LABEL", how="left")
        base["col_total"] = base["col_total"].fillna(0)        # numeric only ✓
        base["value"] = np.where(
            base["col_total"] > 0, base["filt_n"] / base["col_total"] * 100, 0)

    elif metric_mode == 4:
        field_tot = (base.groupby("INSP_APP_FIELD")["filt_n"]
                     .sum().reset_index(name="field_total"))
        base = base.merge(field_tot, on="INSP_APP_FIELD", how="left")
        base["field_total"] = base["field_total"].fillna(0)    # numeric only ✓
        base["value"] = np.where(
            base["field_total"] > 0, base["filt_n"] / base["field_total"] * 100, 0)

    elif metric_mode == 5:
        base = base.merge(_total_m, on="MONTH_LABEL", how="left")
        base["total_m"] = base["total_m"].fillna(0)            # numeric only ✓
        base["value"] = np.where(
            base["total_m"] > 0, base["filt_n"] / base["total_m"] * 100, 0)

    pivot = base.pivot_table(
        index="INSP_APP_FIELD", columns="MONTH_LABEL",
        values="value", aggfunc="sum",
    )
    months_present   = _sort_months([m for m in sel_months if m in pivot.columns])
    pivot            = pivot.reindex(columns=months_present).fillna(0)
    pivot.index.name = "Field"

    pivot.loc["== Total =="] = pivot.sum()
    pivot.loc["== Mean  =="] = pivot.iloc[:-1].mean()
    data_cols      = [c for c in pivot.columns if c not in ("Total", "Mean")]
    pivot["Total"] = pivot[data_cols].sum(axis=1)
    pivot["Mean"]  = pivot[data_cols].mean(axis=1)
    return pivot


# ── Chart functions ───────────────────────────────────────────────────────────
@st.cache_data(show_spinner=False, ttl=None)
def chart_monthly_trend(_row_counts, sel_months_t):
    sel_months = list(sel_months_t)
    df   = _row_counts[_row_counts["MONTH_LABEL"].isin(sel_months)].copy()
    tot  = df.groupby("MONTH_LABEL")["row_cnt"].sum().reset_index(name="total")
    good = (df[df["DS_OUTPUT"] == 0]
            .groupby("MONTH_LABEL")["row_cnt"].sum().reset_index(name="good"))
    m = tot.merge(good, on="MONTH_LABEL", how="left")
    m["good"] = m["good"].fillna(0)                            # numeric only ✓
    m["pct"]  = np.where(m["total"] > 0, m["good"] / m["total"] * 100, 0)
    m["_ord"] = m["MONTH_LABEL"].map({v: i for i, v in enumerate(MONTH_ORDER)})
    m = m.sort_values("_ord")

    fig = px.line(
        m, x="MONTH_LABEL", y="pct", markers=True,
        title="Monthly Quality Trend  (% Field Checks Correct — DS=0)",
        labels={"pct": "% Correct", "MONTH_LABEL": "Month"},
        color_discrete_sequence=["#2ecc71"],
    )
    fig.update_traces(line_width=3, marker_size=10)
    fig.update_layout(height=370, yaxis_range=[0, 100],
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


@st.cache_data(show_spinner=False, ttl=None)
def chart_ds_distribution(_row_counts, sel_months_t):
    sel_months = list(sel_months_t)
    df = _row_counts[_row_counts["MONTH_LABEL"].isin(sel_months)].copy()
    df["DS_label"] = df["DS_OUTPUT"].map(DS_LABELS)
    df["_ord"]     = df["MONTH_LABEL"].map({v: i for i, v in enumerate(MONTH_ORDER)})
    df = df.sort_values("_ord")

    fig = px.bar(
        df, x="MONTH_LABEL", y="row_cnt", color="DS_label", barmode="stack",
        title="DS Output Distribution by Month  (Field Checks / rows)",
        labels={"row_cnt": "Field Checks", "MONTH_LABEL": "Month", "DS_label": "DS Output"},
        color_discrete_map={v: DS_COLORS[k] for k, v in DS_LABELS.items()},
    )
    fig.update_layout(height=370, legend_font_size=10,
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


@st.cache_data(show_spinner=False, ttl=None)
def chart_top_problematic(_core, sel_months_t, sel_fields_t, top_n):
    sel_months = list(sel_months_t)
    sel_fields = list(sel_fields_t)
    df = _core[
        _core["DS_OUTPUT"].isin([1, 3]) &
        _core["MONTH_LABEL"].isin(sel_months) &
        _core["INSP_APP_FIELD"].isin(sel_fields)
    ]
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title=f"Top {top_n} Problematic Fields — no alert data for selection",
                          height=300, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        return fig

    top = (df.groupby("INSP_APP_FIELD")["n"]
           .sum().nlargest(top_n).reset_index(name="cnt"))
    top = top.sort_values("cnt")
    fig = px.bar(
        top, x="cnt", y="INSP_APP_FIELD", orientation="h",
        title=f"Top {top_n} Problematic Fields  (DS=1 or DS=3)",
        labels={"cnt": "Distinct Inspections (alert)", "INSP_APP_FIELD": "Field"},
        color_discrete_sequence=["#e74c3c"],
    )
    fig.update_layout(height=max(380, 28 * len(top) + 80),
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


@st.cache_data(show_spinner=False, ttl=None)
def chart_quality_score(_core, sel_months_t, sel_fields_t):
    sel_months = list(sel_months_t)
    sel_fields = list(sel_fields_t)
    df = _core[
        _core["MONTH_LABEL"].isin(sel_months) &
        _core["INSP_APP_FIELD"].isin(sel_fields)
    ]
    if df.empty:
        fig = go.Figure()
        fig.update_layout(title="Field Quality Score — no data for selection",
                          height=300, paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
        return fig

    tot  = df.groupby("INSP_APP_FIELD")["n"].sum().reset_index(name="total")
    good = (df[df["DS_OUTPUT"] == 0]
            .groupby("INSP_APP_FIELD")["n"].sum().reset_index(name="good"))

    # FIX: merge then fill only the numeric 'good' column.
    # .fillna(0) on the whole frame would try to insert 0 into the
    # INSP_APP_FIELD categorical column → TypeError.
    q = tot.merge(good, on="INSP_APP_FIELD", how="left")
    q["good"]  = q["good"].fillna(0)                          # numeric only ✓
    q["score"] = np.where(q["total"] > 0, q["good"] / q["total"] * 100, 0)
    q = q.sort_values("score")

    fig = px.bar(
        q, x="score", y="INSP_APP_FIELD", orientation="h",
        title="Field Quality Score  (DS=0 / Total  %)",
        labels={"score": "Quality Score (%)", "INSP_APP_FIELD": "Field"},
        color="score", color_continuous_scale="RdYlGn", range_color=[0, 100],
    )
    fig.update_layout(height=max(420, 28 * len(q) + 80), coloraxis_showscale=False,
                      paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)")
    return fig


# ── Heatmap renderer ──────────────────────────────────────────────────────────
def render_heatmap(pivot, metric_mode, colorscale, reverse_color):
    is_pct = metric_mode > 1
    suffix = "%" if is_pct else ""
    fmt    = ".1f" if is_pct else ".0f"

    z     = pivot.values.astype(float)
    x_lbl = [str(c) for c in pivot.columns]
    y_lbl = [str(r) for r in pivot.index]

    AGG_ROW = {"== Total ==", "== Mean  =="}
    AGG_COL = {"Total", "Mean"}

    mask      = np.array([[r not in AGG_ROW and c not in AGG_COL
                           for c in x_lbl] for r in y_lbl])
    data_vals = z[mask]
    zmin = float(data_vals.min()) if data_vals.size else 0.0
    zmax = float(data_vals.max()) if data_vals.size else 1.0
    if zmin == zmax:
        zmax += 1

    cs = colorscale + ("_r" if reverse_color else "")

    anns = []
    for i, rl in enumerate(y_lbl):
        for j, cl in enumerate(x_lbl):
            is_agg = rl in AGG_ROW or cl in AGG_COL
            anns.append(dict(
                x=j, y=i, xref="x", yref="y",
                text=f"{z[i, j]:{fmt}}{suffix}",
                showarrow=False,
                font=dict(size=9, color="white" if is_agg else "#111",
                          family="monospace"),
            ))

    fig = go.Figure(go.Heatmap(
        z=z.tolist(), x=x_lbl, y=y_lbl,
        colorscale=cs, zmin=zmin, zmax=zmax, showscale=True,
        colorbar=dict(title="%" if is_pct else "Count", thickness=14, len=0.75),
        hovertemplate=(f"<b>Field:</b> %{{y}}<br><b>Month:</b> %{{x}}<br>"
                       f"<b>Value:</b> %{{z:.2f}}{suffix}<extra></extra>"),
    ))
    fig.update_layout(
        annotations=anns,
        xaxis=dict(side="top", tickangle=-20, tickfont=dict(size=11)),
        yaxis=dict(autorange="reversed", tickfont=dict(size=10)),
        margin=dict(l=0, r=0, t=50, b=0),
        height=max(520, 26 * len(y_lbl)),
        paper_bgcolor="rgba(0,0,0,0)", plot_bgcolor="rgba(0,0,0,0)",
    )
    return fig


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    inject_css()

    st.markdown("## Inspection DS Quality Control Dashboard")
    st.caption(
        "**Inspection Count = COUNT(DISTINCT Appointment_ID)** — "
        "1 Appointment = 1 Inspection. Multiple rows = multiple fields checked. "
        "DS KPI cards show **Field Checks (rows)**, not inspections."
    )
    st.markdown("---")

    core, total_fm, total_m, row_counts, all_fields = load_all()

    # ── Sidebar ───────────────────────────────────────────────────────────
    with st.sidebar:
        st.markdown("## Filters & Settings")
        st.markdown("---")

        st.markdown("**Month**")
        sel_months = st.multiselect("Months", MONTH_ORDER, default=MONTH_ORDER,
                                    label_visibility="collapsed")
        if not sel_months:
            sel_months = MONTH_ORDER[:]

        st.markdown("**DS Output**")
        ds_opts = {"0 - Correct": 0, "1 - Missed (alert)": 1,
                   "2 - Modified": 2, "3 - Wrong (alert)": 3}
        sel_ds_lbl = st.multiselect("DS Output", list(ds_opts.keys()),
                                    default=list(ds_opts.keys()),
                                    label_visibility="collapsed")
        if not sel_ds_lbl:
            sel_ds_lbl = list(ds_opts.keys())
        ds_filter = [ds_opts[lbl] for lbl in sel_ds_lbl]

        st.markdown("**Field Search**")
        field_search = st.text_input("Field search", value="",
                                     label_visibility="collapsed")
        sel_fields = all_fields[:]
        if field_search:
            sel_fields = [f for f in sel_fields if field_search.lower() in f.lower()]
        if not sel_fields:
            sel_fields = all_fields[:]

        st.markdown("**Metric Mode**")
        metric_mode = st.selectbox(
            "Metric", list(METRIC_LABELS.keys()),
            format_func=lambda x: f"Mode {x}: {METRIC_LABELS[x]}",
            label_visibility="collapsed")

        st.markdown("**Heatmap Color**")
        colorscale = st.selectbox("Scale",
            ["Blues","YlOrRd","Viridis","RdYlGn","Plasma","Cividis"],
            label_visibility="collapsed")
        reverse_color = st.checkbox("Reverse color scale", value=False)

        st.markdown("**Top N Problematic**")
        top_n = st.slider("Top N", 5, 30, 15, label_visibility="collapsed")

        st.markdown("---")
        st.caption("Inspection DS QC Dashboard v6.1  •  Static Data Mode ⚡")

    # ── Cache keys (tuples are hashable) ─────────────────────────────────
    sel_months_t = tuple(_sort_months(sel_months))
    sel_fields_t = tuple(sorted(sel_fields))
    ds_filter_t  = tuple(sorted(ds_filter))

    # ── KPI cards ─────────────────────────────────────────────────────────
    kpi_rows_dict, kpi_insp_total = compute_kpis(row_counts, total_m, sel_months_t)

    def rc(code):
        return int(kpi_rows_dict.get(code, 0))

    total_rows = sum(kpi_rows_dict.values()) or 1
    alert_rows = rc(1) + rc(3)

    st.markdown("#### Overall KPIs")
    c0, _ = st.columns([2, 4])
    with c0:
        st.metric("Total Inspections (Distinct Appt IDs)", f"{kpi_insp_total:,}",
                  help="COUNT(DISTINCT APPOINTMENT_ID) for selected months.")

    st.caption("Field Check counts (rows) — one inspection checks ~20-30 fields, "
               "so these counts are ~20-30× larger than inspection count.")

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Correct (DS=0)",      f"{rc(0):,}", f"{rc(0)/total_rows*100:.1f}% of checks")
    c2.metric("Missed (DS=1)",       f"{rc(1):,}", f"{rc(1)/total_rows*100:.1f}% of checks")
    c3.metric("Modified (DS=2)",     f"{rc(2):,}", f"{rc(2)/total_rows*100:.1f}% of checks")
    c4.metric("Wrong (DS=3)",        f"{rc(3):,}", f"{rc(3)/total_rows*100:.1f}% of checks")
    c5.metric("Alert Rate (DS=1+3)", f"{alert_rows/total_rows*100:.1f}%",
              f"{alert_rows:,} checks")

    st.caption(f"{kpi_insp_total:,} unique inspections | {total_rows:,} total field checks | "
               f"{len(sel_months)} month(s) | {len(sel_fields)} field(s) selected.")
    st.markdown("---")

    # ── Pivot heatmap ──────────────────────────────────────────────────────
    st.markdown(f"### Pivot Heatmap — Mode {metric_mode}: {METRIC_LABELS[metric_mode]}")
    if metric_mode == 3:
        st.info("**Mode 3 denominator** = sum of selected-field inspection counts for that month.",
                icon="ℹ️")

    pivot = compute_pivot(core, total_fm, total_m,
                          sel_months_t, sel_fields_t, ds_filter_t, metric_mode)

    if pivot.empty or pivot.shape[0] <= 2:
        st.warning("No data for current selection.")
    else:
        st.plotly_chart(render_heatmap(pivot, metric_mode, colorscale, reverse_color),
                        use_container_width=True)
        buf = io.StringIO()
        pivot.to_csv(buf)
        st.download_button("Export Pivot to CSV", data=buf.getvalue(),
                           file_name="pivot_export.csv", mime="text/csv")

    st.markdown("---")

    # ── Analytical charts ──────────────────────────────────────────────────
    st.markdown("### Analytical Insights")

    cl, cr = st.columns(2)
    with cl:
        st.plotly_chart(chart_monthly_trend(row_counts, sel_months_t),
                        use_container_width=True)
    with cr:
        st.plotly_chart(chart_ds_distribution(row_counts, sel_months_t),
                        use_container_width=True)

    cl2, cr2 = st.columns(2)
    with cl2:
        st.plotly_chart(chart_top_problematic(core, sel_months_t, sel_fields_t, top_n),
                        use_container_width=True)
    with cr2:
        st.plotly_chart(chart_quality_score(core, sel_months_t, sel_fields_t),
                        use_container_width=True)

    st.markdown("---")

    # ── Raw pivot expander ─────────────────────────────────────────────────
    with st.expander("Raw Pivot Table"):
        is_pct     = metric_mode > 1
        display_df = pivot.reset_index()
        col_cfg    = {}
        for col in display_df.columns:
            if col == "Field":
                col_cfg[col] = st.column_config.TextColumn(col, width="medium")
            elif is_pct:
                col_cfg[col] = st.column_config.NumberColumn(col, format="%.2f%%", min_value=0)
            else:
                col_cfg[col] = st.column_config.NumberColumn(col, format="%d", min_value=0)
        st.dataframe(display_df, column_config=col_cfg,
                     use_container_width=True, height=480, hide_index=True)

    st.markdown(
        "<br><center><sub>Inspection DS QC Dashboard v6.1  |  Streamlit + Plotly  |  "
        "Static Data Mode — pre-aggregated for instant performance ⚡</sub></center>",
        unsafe_allow_html=True,
    )


if __name__ == "__main__":
    main()