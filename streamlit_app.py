from __future__ import annotations

from datetime import datetime
from io import BytesIO

import pandas as pd
import streamlit as st
from dotenv import load_dotenv
from streamlit.delta_generator import DeltaGenerator

from app.config import get_settings
from app.dashboard import components
from app.data_processing.cleaning import (
    apply_filters,
    apply_period_filters,
    annotate_period_columns,
    category_breakdown,
    clean_transactions,
    compute_kpis,
    get_filter_options,
    get_period_options,
    get_recent_messages,
    load_transactions,
    read_vs_unread_by_hour,
    build_status_breakdown,
)
from app.models.report import ReportMetadata, ReportMetrics
from app.services.supabase_service import ReportStore

load_dotenv()
st.set_page_config(page_title="WA Blast Dashboard", layout="wide")


def inject_custom_styles() -> None:
    st.markdown(
        """
        <style>
            .main {
                background: linear-gradient(135deg, #04141d 0%, #0a2c2d 50%, #04141d 100%);
                color: #e2f7e6;
            }
            .block-container {
                padding-top: 1.5rem;
            }
            .budget-hero {
                background: rgba(7, 23, 29, 0.9);
                border-radius: 20px;
                padding: 24px 32px;
                margin-bottom: 12px;
                box-shadow: inset 0 0 40px rgba(0, 255, 200, 0.05);
                display:flex;
                justify-content:space-between;
                align-items:center;
            }
            .budget-hero h1 {
                margin-bottom: 6px;
            }
            .hero-meta {
                text-align:right;
            }
            .control-panel-heading {
                background: rgba(5, 20, 26, 0.85);
                border-radius: 18px;
                border: 1px solid rgba(34, 211, 238, 0.35);
                padding: 18px 24px;
                margin-bottom: 0.9rem;
                box-shadow: 0 10px 25px rgba(0,0,0,0.35);
            }
            .control-panel-heading .control-eyebrow {
                text-transform: uppercase;
                font-size: 12px;
                letter-spacing: 0.15em;
                color: #5eead4;
                display:block;
                margin-bottom:4px;
            }
            .control-panel-heading h4 {
                margin: 0;
                color: #e2f7e6;
            }
            .control-panel-heading p {
                margin: 4px 0 0;
                color: #a5f3fc;
                font-size: 14px;
            }
            .panel-note {
                color: #c4f1f9;
                font-size: 14px;
                margin-bottom: 4px;
            }
            .control-instruction {
                color: #a5f3fc;
                font-size: 13px;
                padding-left: 18px;
                margin-bottom: 12px;
            }
            .control-instruction li {
                margin-bottom: 4px;
            }
            .period-filter-label {
                font-size: 13px;
                text-transform: uppercase;
                letter-spacing: 0.18em;
                color:#67e8f9;
                margin: 10px 0 4px;
            }
            div[data-testid="column"] > div:has([data-testid="stFileUploader"]) {
                display:flex;
                flex-direction:column;
                justify-content:center;
                gap:10px;
            }
            div[data-testid="column"] > div:has(.panel-note) {
                display:flex;
                flex-direction:column;
                justify-content:center;
                gap:10px;
            }
            .budget-card {
                background: rgba(7, 30, 32, 0.85);
                padding: 18px;
                border-radius: 18px;
                border: 1px solid rgba(22, 163, 74, 0.4);
                box-shadow: 0 12px 30px rgba(0,0,0,0.35);
            }
            .budget-card p {
                margin: 0;
                font-size: 14px;
                text-transform: uppercase;
                letter-spacing: 0.1em;
                color: #93e6d5;
            }
            .budget-card h2 {
                margin: 8px 0;
                font-size: 32px;
                color: #e2f7e6;
            }
            .budget-card span {
                font-size: 13px;
                color: #a7f3d0;
            }
            .stRadio div[role="radiogroup"] {
                display:flex;
                flex-wrap:wrap;
                gap:10px;
                margin-bottom:6px;
            }
            .stRadio div[role="radiogroup"] label {
                background:#020617;
                padding:10px 20px;
                border-radius:12px;
                border:1px solid rgba(148,163,184,0.35);
                color:#f8fafc;
                min-width:90px;
                text-align:center;
                font-weight:500;
                box-shadow:inset 0 0 15px rgba(15,118,110,0.25);
            }
            .stRadio div[role="radiogroup"] label[data-checked="true"] {
                background:linear-gradient(120deg,#16a34a,#22d3ee);
                color:#04141d;
                border-color:rgba(45,212,191,0.9);
                box-shadow:0 0 18px rgba(34,197,94,0.35);
            }
            [data-testid="stFileUploader"] {
                background: rgba(7,30,32,0.85);
                border:1px dashed rgba(34,211,238,0.6);
                padding:16px;
                border-radius:18px;
            }
            [data-testid="stFileUploader"] label {
                color:#a5f3fc;
            }
            .stButton>button {
                background: linear-gradient(90deg, #059669, #22d3ee);
                color:#04141d;
                border:none;
                border-radius:999px;
                font-weight:600;
                width:100%;
                min-height:46px;
                box-shadow:0 8px 20px rgba(16,185,129,0.35);
            }
            .stButton>button[data-testid="baseButton-secondary"] {
                background:#010b13;
                color:#f8fafc;
                border:1px solid rgba(148,163,184,0.35);
                box-shadow:none;
            }
            .stButton>button:disabled {
                background:rgba(55,65,81,0.7) !important;
                color:#94a3b8 !important;
                border-color:rgba(148,163,184,0.3) !important;
                cursor:not-allowed !important;
                box-shadow:none !important;
            }
        </style>
        """,
        unsafe_allow_html=True,
    )


@st.cache_resource
def init_services() -> tuple:
    settings = get_settings()
    store = ReportStore(settings)
    return settings, store


settings, report_store = init_services()
inject_custom_styles()


@st.cache_data(ttl=90, show_spinner=False, hash_funcs={ReportStore: lambda _: None})
def load_cached_dataset(store: ReportStore, report_id: str | None, version: int) -> tuple[pd.DataFrame, dict | None]:
    if report_id:
        df, metadata = store.dataset_by_id(report_id)
    else:
        df, metadata = store.latest_dataset()
    metadata_dict = metadata.serialize() if metadata else None
    return df, metadata_dict


def refresh_dataset_version() -> int:
    st.session_state.setdefault("dataset_version", 0)
    return st.session_state["dataset_version"]


def increment_dataset_version() -> None:
    st.session_state["dataset_version"] = refresh_dataset_version() + 1


def upload_controls(store: ReportStore, container: DeltaGenerator) -> None:
    container.markdown(
        """
        <div class="control-panel-heading">
            <span class="control-eyebrow">Sumber Data</span>
            <h4>Upload Laporan Terbaru</h4>
            <p>Segarkan metrik dashboard dengan laporan CSV/XLSX terbaru dari BSP.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    upload_col, helper_col = container.columns([3, 2], gap="large")
    with upload_col:
        uploaded = st.file_uploader(
            "Unggah CSV/XLSX BSP", type=["csv", "xlsx"], key="top_uploader", label_visibility="collapsed"
        )
        st.caption("Format CSV/XLSX · maks 25MB · timezone UTC")
    with helper_col:
        st.markdown(
            """
            <p class="panel-note">Tips agar proses berjalan lancar:</p>
            <ul class="control-instruction">
                <li>Gunakan file export langsung dari WhatsApp BSP report center.</li>
                <li>Hindari mengubah nama kolom atau struktur aslinya.</li>
                <li>Klik tombol di bawah ini setelah memilih file untuk menyimpan & merefresh dashboard.</li>
            </ul>
            """,
            unsafe_allow_html=True,
        )
        process = st.button("Upload & Proses", use_container_width=True, type="primary")
        st.caption("File akan dibersihkan otomatis dan disimpan sebagai dataset terbaru.")

    if not process:
        return

    if uploaded is None:
        container.error("Pilih file terlebih dahulu.")
        return

    try:
        raw_bytes = uploaded.getvalue()
        raw_df = load_transactions(BytesIO(raw_bytes), filename=uploaded.name)
        cleaned_df = clean_transactions(raw_df)
        kpis = compute_kpis(cleaned_df)
        metrics = ReportMetrics(
            total_messages=kpis["total"],
            delivered=kpis["delivered"],
            failed=kpis["failed"],
            read=kpis["read"],
            delivery_rate=kpis["delivery_rate"],
            read_rate=kpis["read_rate"],
            avg_rate_value=kpis["avg_rate"],
        )
        metadata = ReportMetadata(
            period_label=datetime.utcnow().strftime("%Y-%m-%d %H:%M"),
            source_filename=uploaded.name,
            uploaded_by="auto-uploader",
            metrics=metrics,
        )
        store.persist_report(raw_bytes, cleaned_df, metadata)
        increment_dataset_version()
        container.success("Laporan berhasil disimpan dan dashboard diperbarui.")
        st.rerun()
    except Exception as exc:  # pragma: no cover - UI feedback
        container.error(f"Gagal memproses file: {exc}")


def main() -> None:
    upload_controls(report_store, st.container())

    dataset_version = refresh_dataset_version()

    try:
        df, metadata_payload = load_cached_dataset(report_store, None, dataset_version)
        metadata = ReportMetadata.from_dict(metadata_payload) if metadata_payload else None
    except FileNotFoundError:
        st.warning("Belum ada dataset. Unggah file pertama Anda melalui tombol di kiri atas.")
        return

    df = annotate_period_columns(df)
    components.render_header(metadata)

    period_options = get_period_options(df)
    year_filter, month_filter, week_filter = components.render_period_selector(
        period_options.get("years", []), period_options.get("months_by_year", {})
    )
    period_filtered_df = apply_period_filters(df, year_filter, month_filter, week_filter)

    selectors = get_filter_options(period_filtered_df)
    template_filter = components.render_template_filter(selectors.get("templates", []))
    filtered_df = apply_filters(period_filtered_df, template=template_filter)
    if filtered_df.empty:
        st.warning("Filter tidak menemukan data. Menampilkan seluruh dataset.")
        filtered_df = period_filtered_df if not period_filtered_df.empty else df

    kpis = compute_kpis(filtered_df)
    status_df = build_status_breakdown(filtered_df)
    category_df = category_breakdown(filtered_df)
    read_activity_df = read_vs_unread_by_hour(filtered_df)
    recent_df = get_recent_messages(filtered_df)

    components.render_kpi_cards(kpis)

    col_left, col_right = st.columns(2)
    with col_left:
        components.render_category_donut(category_df)
    with col_right:
        components.render_status_comparison(status_df)

    components.render_read_activity(read_activity_df)

    components.render_cost_summary(category_df)

    st.divider()
    st.subheader("Pesan Terbaru")
    st.dataframe(recent_df, use_container_width=True, hide_index=True)

    with st.expander("Detail dataset bersih"):
        st.dataframe(filtered_df, use_container_width=True)
        csv_data = filtered_df.to_csv(index=False).encode("utf-8")
        st.download_button(
            "Unduh CSV bersih",
            data=csv_data,
            file_name=f"wa-report-{metadata.period_label}.csv",
            mime="text/csv",
        )

if __name__ == "__main__":
    main()
