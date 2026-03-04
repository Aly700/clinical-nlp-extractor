from __future__ import annotations

import json
from collections import Counter
from datetime import date

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, inspect
from sqlalchemy.exc import SQLAlchemyError

DB_URL = "sqlite:///musicallite.db"
EXPECTED_TABLES = [
    "patients",
    "visits",
    "medications",
    "mri_results",
    "quarantine_records",
    "audit_logs",
]
QUARANTINE_FILTER_OPTIONS = ["edss_out_of_range", "future_year"]
QUARANTINE_EMPTY_MESSAGE = "No quarantined records (good run)."
QUARANTINE_HINT = (
    "To demo quarantine, run: python -m src.ingest --input data/sample_reports_bad "
    "--db sqlite:///musicallite.db"
)
JSON_COLUMNS = ["details_json", "errors_json", "extracted_json"]


@st.cache_data(show_spinner=False)
def run_query(db_url: str, query: str) -> pd.DataFrame:
    engine = create_engine(db_url)
    return pd.read_sql_query(query, engine)


@st.cache_data(show_spinner=False)
def get_table_names(db_url: str) -> list[str]:
    engine = create_engine(db_url)
    with engine.connect() as conn:
        return inspect(conn).get_table_names()


def load_table(db_url: str, table_name: str, order_by: str | None = None, limit: int | None = None) -> pd.DataFrame:
    query = f"SELECT * FROM {table_name}"
    if order_by:
        query += f" ORDER BY {order_by}"
    if limit is not None:
        query += f" LIMIT {int(limit)}"
    return run_query(db_url, query)


def load_count(db_url: str, table_name: str) -> int:
    df = run_query(db_url, f"SELECT COUNT(*) AS count_value FROM {table_name}")
    return int(df.iloc[0]["count_value"]) if not df.empty else 0


def parse_quarantine_reason_labels(errors_json: str | None) -> list[str]:
    if not errors_json:
        return []
    try:
        payload = json.loads(errors_json)
    except json.JSONDecodeError:
        return []
    if not isinstance(payload, list):
        return []

    labels: list[str] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        field = str(item.get("field", "")).strip().lower()
        reason = str(item.get("reason", "")).strip().lower()
        if field == "edss" and reason == "out_of_range":
            labels.append("edss_out_of_range")
        if reason == "future_year":
            labels.append("future_year")
    return sorted(set(labels))


def format_reason_label(reason_key: str) -> str:
    words = reason_key.replace("_", " ").split()
    return " ".join("EDSS" if word.lower() == "edss" else word.title() for word in words)


def _pretty_json(value):
    if value is None:
        return value
    if isinstance(value, float) and pd.isna(value):
        return value
    if not isinstance(value, str):
        return value
    stripped = value.strip()
    if not stripped:
        return value
    try:
        payload = json.loads(stripped)
    except json.JSONDecodeError:
        return value
    return json.dumps(payload, indent=2, sort_keys=True)


def _format_date_columns(df: pd.DataFrame) -> pd.DataFrame:
    out = df.copy()
    for col in out.columns:
        lower = col.lower()
        if lower.endswith("_date") or lower.endswith("_at"):
            parsed = pd.to_datetime(out[col], errors="coerce")
            if parsed.notna().any():
                out[col] = parsed.dt.strftime("%Y-%m-%d").where(parsed.notna(), out[col])
    return out


def prepare_display_df(df: pd.DataFrame, show_raw_text: bool, expand_long_text: bool) -> pd.DataFrame:
    out = df.copy()
    if not expand_long_text:
        drop_cols = [col for col in ["raw_text", *JSON_COLUMNS] if col in out.columns]
        if drop_cols:
            out = out.drop(columns=drop_cols)
    else:
        for col in JSON_COLUMNS:
            if col in out.columns:
                out[col] = out[col].apply(_pretty_json)
        if not show_raw_text and "raw_text" in out.columns:
            out = out.drop(columns=["raw_text"])
    return _format_date_columns(out)


def render_table_section(
    title: str,
    df: pd.DataFrame,
    show_raw_text: bool,
    expand_long_text: bool,
    csv_filename: str,
    empty_message: str = "No rows found.",
    empty_hint: str | None = None,
) -> None:
    st.subheader(title)
    if df.empty:
        st.info(empty_message)
        if empty_hint:
            st.caption(empty_hint)
        return
    st.download_button(
        "Download CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name=csv_filename,
        mime="text/csv",
        key=f"download-{csv_filename}",
    )
    st.dataframe(
        prepare_display_df(df, show_raw_text, expand_long_text),
        use_container_width=True,
        hide_index=True,
    )


def main() -> None:
    st.set_page_config(page_title="Clinical NLP Extractor Dashboard", layout="wide")
    st.title("Clinical NLP Extractor Dashboard")

    st.sidebar.header("Filters")
    db_url = st.sidebar.text_input("Database URL", value=DB_URL)
    if st.sidebar.button("Refresh"):
        st.cache_data.clear()
        st.rerun()

    expand_long_text = st.sidebar.checkbox(
        "Expand long text columns (raw_text/details_json)",
        value=False,
    )
    show_raw_text = st.sidebar.checkbox(
        "Show raw_text columns",
        value=False,
        disabled=not expand_long_text,
    )

    quarantine_reason_filter = st.sidebar.multiselect(
        "Quarantine error types",
        options=QUARANTINE_FILTER_OPTIONS,
        default=[],
        format_func=format_reason_label,
        help="If selected, only quarantined records with these error types are shown.",
    )

    try:
        table_names = get_table_names(db_url)
    except SQLAlchemyError as exc:
        st.error(f"Could not connect to database: {exc}")
        return

    if not table_names:
        st.warning("No database tables found.")
        st.caption("Run ingestion once to initialize schema and load sample data:")
        st.code("python -m src.ingest --input data/sample_reports --db sqlite:///musicallite.db", language="bash")
        return

    data: dict[str, pd.DataFrame] = {}
    for table in EXPECTED_TABLES:
        if table not in table_names:
            data[table] = pd.DataFrame()
            continue
        try:
            if table == "audit_logs":
                data[table] = load_table(db_url, table, order_by="created_at DESC, id DESC", limit=20)
            else:
                data[table] = load_table(db_url, table, order_by="id")
        except SQLAlchemyError as exc:
            st.error(f"Could not query table '{table}': {exc}")
            data[table] = pd.DataFrame()

    counts: dict[str, int] = {}
    for table in EXPECTED_TABLES:
        if table in table_names:
            try:
                counts[table] = load_count(db_url, table)
            except SQLAlchemyError:
                counts[table] = len(data.get(table, pd.DataFrame()))
        else:
            counts[table] = 0

    patients_df = data["patients"].copy()
    visits_df = data["visits"].copy()
    medications_df = data["medications"].copy()
    mri_df = data["mri_results"].copy()
    quarantine_df = data["quarantine_records"].copy()
    audit_latest_df = data["audit_logs"].copy()

    mrn_options = ["All"]
    if not patients_df.empty and "mrn" in patients_df.columns:
        mrn_options.extend(sorted({str(v) for v in patients_df["mrn"].dropna().tolist()}))
    selected_mrn = st.sidebar.selectbox("Patient MRN", options=mrn_options, index=0)

    date_filter_active = False
    selected_start: date | None = None
    selected_end: date | None = None
    if not visits_df.empty and "visit_date" in visits_df.columns:
        parsed_dates = pd.to_datetime(visits_df["visit_date"], errors="coerce").dt.date
        valid_dates = parsed_dates.dropna()
        if not valid_dates.empty:
            min_date = valid_dates.min()
            max_date = valid_dates.max()
            selected_date_range = st.sidebar.date_input(
                "Visit date range",
                value=(min_date, max_date),
                min_value=min_date,
                max_value=max_date,
            )
            if isinstance(selected_date_range, tuple) and len(selected_date_range) == 2:
                selected_start, selected_end = selected_date_range
                date_filter_active = True
            visits_df["visit_date_parsed"] = parsed_dates

    if selected_mrn != "All":
        selected_patient_ids = set()
        if not patients_df.empty and "id" in patients_df.columns and "mrn" in patients_df.columns:
            selected_patient_ids = set(patients_df.loc[patients_df["mrn"] == selected_mrn, "id"].tolist())
            patients_df = patients_df.loc[patients_df["mrn"] == selected_mrn].copy()
        if not visits_df.empty and "patient_id" in visits_df.columns:
            visits_df = visits_df.loc[visits_df["patient_id"].isin(selected_patient_ids)].copy()

    if date_filter_active and selected_start is not None and selected_end is not None:
        if "visit_date_parsed" in visits_df.columns:
            visits_df = visits_df.loc[
                visits_df["visit_date_parsed"].notna()
                & (visits_df["visit_date_parsed"] >= selected_start)
                & (visits_df["visit_date_parsed"] <= selected_end)
            ].copy()
            visits_df = visits_df.drop(columns=["visit_date_parsed"], errors="ignore")

        if not patients_df.empty and "id" in patients_df.columns and "patient_id" in visits_df.columns:
            valid_patient_ids = set(visits_df["patient_id"].dropna().tolist())
            patients_df = patients_df.loc[patients_df["id"].isin(valid_patient_ids)].copy()

    if not visits_df.empty and "visit_date" in visits_df.columns:
        visit_sort = pd.to_datetime(visits_df["visit_date"], errors="coerce")
        visits_df = visits_df.assign(_visit_sort=visit_sort)
        sort_by = ["_visit_sort"]
        ascending = [False]
        if "id" in visits_df.columns:
            sort_by.append("id")
            ascending.append(False)
        visits_df = visits_df.sort_values(by=sort_by, ascending=ascending, na_position="last").drop(
            columns=["_visit_sort"]
        )

    visit_ids = set(visits_df["id"].tolist()) if not visits_df.empty and "id" in visits_df.columns else set()

    if not medications_df.empty and "visit_id" in medications_df.columns and visit_ids:
        medications_df = medications_df.loc[medications_df["visit_id"].isin(visit_ids)].copy()
    elif not visits_df.empty and "id" in visits_df.columns and medications_df.empty:
        medications_df = medications_df.copy()
    elif not visit_ids:
        medications_df = medications_df.iloc[0:0].copy()

    if not mri_df.empty and "visit_id" in mri_df.columns and visit_ids:
        mri_df = mri_df.loc[mri_df["visit_id"].isin(visit_ids)].copy()
    elif not visits_df.empty and "id" in visits_df.columns and mri_df.empty:
        mri_df = mri_df.copy()
    elif not visit_ids:
        mri_df = mri_df.iloc[0:0].copy()

    if not quarantine_df.empty and "errors_json" in quarantine_df.columns:
        quarantine_df["reason_labels"] = quarantine_df["errors_json"].apply(parse_quarantine_reason_labels)
        quarantine_df["reason_summary"] = quarantine_df["reason_labels"].apply(lambda values: ", ".join(values))
        if quarantine_reason_filter:
            wanted = set(quarantine_reason_filter)
            quarantine_df = quarantine_df.loc[
                quarantine_df["reason_labels"].apply(lambda values: bool(wanted.intersection(set(values))))
            ].copy()
        quarantine_df = quarantine_df.drop(columns=["reason_labels"], errors="ignore")
    else:
        quarantine_df["reason_summary"] = ""

    if not audit_latest_df.empty and "created_at" in audit_latest_df.columns:
        audit_sort = pd.to_datetime(audit_latest_df["created_at"], errors="coerce")
        audit_latest_df = audit_latest_df.assign(_audit_sort=audit_sort)
        sort_by = ["_audit_sort"]
        ascending = [False]
        if "id" in audit_latest_df.columns:
            sort_by.append("id")
            ascending.append(False)
        audit_latest_df = audit_latest_df.sort_values(by=sort_by, ascending=ascending, na_position="last").drop(
            columns=["_audit_sort"]
        )

    st.subheader("Summary")
    visits_all_df = data["visits"]
    total_visits = len(visits_all_df)
    edss_pct = (
        (visits_all_df["edss"].notna().sum() / total_visits * 100.0)
        if total_visits and "edss" in visits_all_df.columns
        else 0.0
    )
    dmt_pct = (
        (visits_all_df["current_dmt"].notna().sum() / total_visits * 100.0)
        if total_visits and "current_dmt" in visits_all_df.columns
        else 0.0
    )
    lesion_pct = (
        (visits_all_df["mri_new_lesions_count"].notna().sum() / total_visits * 100.0)
        if total_visits and "mri_new_lesions_count" in visits_all_df.columns
        else 0.0
    )

    metric_items = [
        ("Total patients", counts.get("patients", 0)),
        ("Total visits", counts.get("visits", 0)),
        ("Total medications", counts.get("medications", 0)),
        ("Total MRI results", counts.get("mri_results", 0)),
        ("Total quarantined records", counts.get("quarantine_records", 0)),
        ("Total audit logs", counts.get("audit_logs", 0)),
        ("% visits with EDSS", f"{edss_pct:.1f}%"),
        ("% visits with current_dmt", f"{dmt_pct:.1f}%"),
        ("% visits with mri_new_lesions_count", f"{lesion_pct:.1f}%"),
    ]

    metric_cols = st.columns(3)
    for idx, (label, value) in enumerate(metric_items):
        metric_cols[idx % 3].metric(label=label, value=value)

    st.subheader("Charts")
    chart_cols = st.columns(2)

    with chart_cols[0]:
        st.caption("MRI results by body_site")
        if not mri_df.empty and "body_site" in mri_df.columns:
            mri_counts = (
                mri_df["body_site"]
                .fillna("Unknown")
                .astype(str)
                .str.strip()
                .replace("", "Unknown")
                .value_counts()
                .rename_axis("body_site")
                .reset_index(name="count")
                .set_index("body_site")
            )
            st.bar_chart(mri_counts)
        else:
            st.info("No MRI rows available for chart.")

    with chart_cols[1]:
        st.caption("Quarantine reasons frequency")
        reason_counter = Counter()
        if not quarantine_df.empty and "errors_json" in quarantine_df.columns:
            for labels in quarantine_df["errors_json"].apply(parse_quarantine_reason_labels):
                for label in labels:
                    reason_counter[label] += 1
        if reason_counter:
            reason_df = (
                pd.DataFrame(
                    [
                        {
                            "reason_key": reason,
                            "reason_label": format_reason_label(reason),
                            "count": count,
                        }
                        for reason, count in reason_counter.items()
                    ]
                )
                .sort_values("count", ascending=False)
                .set_index("reason_label")
            )
            st.bar_chart(reason_df)
        else:
            st.info(QUARANTINE_EMPTY_MESSAGE)
            st.caption(QUARANTINE_HINT)

    render_table_section("Section 1: Patients", patients_df, show_raw_text, expand_long_text, "patients.csv")
    render_table_section("Section 2: Visits", visits_df, show_raw_text, expand_long_text, "visits.csv")
    render_table_section("Section 3: Medications", medications_df, show_raw_text, expand_long_text, "medications.csv")
    render_table_section("Section 4: MRI Results", mri_df, show_raw_text, expand_long_text, "mri_results.csv")
    render_table_section(
        "Section 5: Quarantine Records",
        quarantine_df,
        show_raw_text,
        expand_long_text,
        "quarantine_records.csv",
        empty_message=QUARANTINE_EMPTY_MESSAGE,
        empty_hint=QUARANTINE_HINT,
    )
    render_table_section("Section 6: Audit Logs", audit_latest_df, show_raw_text, expand_long_text, "audit_logs.csv")


if __name__ == "__main__":
    main()
