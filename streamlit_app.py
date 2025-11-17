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
            @import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700&family=Space+Mono:wght@400;700&display=swap');
            :root {
                --bg-app: #01030d;
                --bg-panel: rgba(5,10,25,0.92);
                --bg-muted: rgba(15,23,42,0.78);
                --border-subtle: rgba(148,163,184,0.25);
                --text-primary: #f8fafc;
                --text-muted: #94a3b8;
                --accent-cyan: #22d3ee;
                --accent-blue: #3b82f6;
                --accent-indigo: #818cf8;
                --accent-pink: #f472b6;
                --accent-green: #34d399;
            }
            html, body, [data-testid="stAppViewContainer"], .stApp {
                background: radial-gradient(circle at top, #06112a 0%, #030712 55%, #01030d 100%);
                color: var(--text-primary);
                font-family: 'Inter', sans-serif;
            }
            .block-container {
                padding: 2.8rem 5vw 3.4rem;
                max-width: 1320px;
                margin-top: 2rem;
            }
            section[data-testid="stSidebar"] { display: none; }
            .panel-heading {
                font-weight: 600;
                letter-spacing: 0.08em;
                text-transform: uppercase;
                font-size: 0.85rem;
                color: var(--text-muted);
                margin-bottom: 0.35rem;
            }
            .panel-heading strong {
                display: block;
                text-transform: none;
                letter-spacing: normal;
                font-size: 1.12rem;
                color: var(--text-primary);
            }
            .panel-description {
                margin-top: -0.1rem;
                font-size: 0.9rem;
                color: var(--text-muted);
            }
            [data-testid="stVerticalBlock"]:has(.panel-heading) {
                background: var(--bg-panel);
                border: 1px solid var(--border-subtle);
                border-radius: 28px;
                padding: 1.6rem 2.1rem 1.7rem;
                box-shadow: 0 30px 75px rgba(2,6,23,0.55);
                margin-bottom: 1.5rem;
            }
            [data-testid="stVerticalBlock"]:has(.hero-card) {
                background: transparent;
                border: none;
                padding: 0;
                box-shadow: none;
            }
            .hero-card {
                display: flex;
                justify-content: space-between;
                gap: 2.8rem;
                align-items: center;
                padding: 2.6rem 3rem;
                border-radius: 36px;
                background: linear-gradient(135deg, rgba(8,47,73,0.85), rgba(15,23,42,0.9));
                border: 1px solid rgba(56,189,248,0.25);
                box-shadow: 0 40px 110px rgba(2,6,23,0.65);
                margin-bottom: 2rem;
            }
            .hero-eyebrow {
                font-size: 0.78rem;
                letter-spacing: 0.25em;
                text-transform: uppercase;
                color: rgba(148,163,184,0.8);
                margin-bottom: 0.2rem;
            }
            .hero-card h1 {
                margin: 0.4rem 0;
                font-size: 2.2rem;
                color: #f8fafc;
            }
            .hero-description {
                margin: 0;
                color: var(--text-muted);
                max-width: 540px;
            }
            .hero-meta {
                min-width: 240px;
                text-align: right;
                border-left: 1px solid rgba(148,163,184,0.25);
                padding-left: 1.8rem;
            }
            .hero-meta-label {
                font-size: 0.75rem;
                letter-spacing: 0.2em;
                text-transform: uppercase;
                color: rgba(148,163,184,0.85);
            }
            .hero-meta-value {
                font-size: 1.55rem;
                font-family: 'Space Mono', monospace;
                margin: 0.2rem 0;
            }
            .hero-updated {
                font-size: 0.8rem;
                color: var(--text-muted);
            }
            .metrics-grid {
                display: grid;
                grid-template-columns: repeat(auto-fit, minmax(190px, 1fr));
                gap: 18px;
                margin-top: 1.4rem;
            }
            .kpi-summary-tag {
                display: inline-flex;
                align-items: center;
                gap: 0.35rem;
                padding: 0.35rem 0.9rem;
                border-radius: 999px;
                background: rgba(59,130,246,0.18);
                color: var(--text-primary);
                font-size: 0.8rem;
                letter-spacing: 0.04em;
                text-transform: uppercase;
                margin-bottom: 0.8rem;
                border: 1px solid rgba(59,130,246,0.35);
            }
            .kpi-card {
                border-radius: 22px;
                padding: 1.5rem 1.6rem;
                background: linear-gradient(135deg, rgba(2,8,23,0.9), rgba(15,23,42,0.8));
                border: 1px solid rgba(148,163,184,0.18);
                box-shadow: inset 0 0 0 1px rgba(255,255,255,0.02);
                margin-bottom: 1.2rem;
            }
            .kpi-card.accent-cyan { border-color: rgba(34,211,238,0.45); background: radial-gradient(circle at top, rgba(34,211,238,0.22), rgba(15,23,42,0.85)); }
            .kpi-card.accent-blue { border-color: rgba(129,140,248,0.35); background: radial-gradient(circle at top, rgba(129,140,248,0.2), rgba(15,23,42,0.85)); }
            .kpi-card.accent-pink { border-color: rgba(244,114,182,0.35); background: radial-gradient(circle at top, rgba(244,114,182,0.2), rgba(15,23,42,0.85)); }
            .kpi-card.accent-amber { border-color: rgba(251,191,36,0.35); background: radial-gradient(circle at top, rgba(251,191,36,0.2), rgba(15,23,42,0.85)); }
            .kpi-card.accent-slate { border-color: rgba(148,163,184,0.35); }
            .kpi-label {
                font-size: 0.78rem;
                text-transform: uppercase;
                letter-spacing: 0.12em;
                color: rgba(148,163,184,0.9);
                margin-bottom: 0.4rem;
            }
            .kpi-value {
                font-size: 2rem;
                margin: 0;
                font-weight: 600;
            }
            .kpi-meta {
                margin-top: 0.2rem;
                font-size: 0.87rem;
                color: var(--text-muted);
                display: flex;
                justify-content: space-between;
                align-items: center;
            }
            .kpi-meta small {
                text-transform: uppercase;
                letter-spacing: 0.08em;
                font-size: 0.74rem;
                color: var(--text-muted);
            }
            .kpi-meta span {
                font-weight: 600;
                color: #a5f3fc;
            }
            .input-label {
                font-size: 0.74rem;
                letter-spacing: 0.1em;
                text-transform: uppercase;
                color: var(--text-muted);
                margin-bottom: 0.3rem;
            }
            [data-baseweb="select"] > div {
                background: var(--bg-muted);
                border: 1px solid var(--border-subtle);
                border-radius: 16px;
                min-height: 58px;
                padding: 0 1rem !important;
                display: flex;
                align-items: center;
                color: var(--text-primary);
            }
            [data-baseweb="select"] svg { color: var(--text-muted); }
            .stButton > button {
                background: linear-gradient(120deg, var(--accent-cyan), var(--accent-blue));
                border: none;
                border-radius: 999px;
                color: #041424;
                font-weight: 600;
                height: 52px;
                box-shadow: 0 18px 40px rgba(37, 99, 235, 0.35);
            }
            [data-testid="stFileUploader"] {
                background: transparent;
            }
            [data-testid="stFileUploaderDropzone"] {
                border: 1px dashed rgba(148,163,184,0.35);
                border-radius: 18px;
                background: rgba(15,23,42,0.75);
            }
            .viz-center {
                display: flex;
                align-items: center;
                justify-content: center;
                min-height: 320px;
                width: 100%;
                height: 100%;
            }
            .viz-center > div {
                width: 100%;
                display: flex;
                align-items: center;
                justify-content: center;
            }
            .viz-center div[data-testid="stPlotlyChart"],
            .viz-center div[data-testid="stVegaLiteChart"] {
                display: flex;
                align-items: center;
                justify-content: center;
                width: 100%;
                height: 100%;
            }
            div[data-testid="stDataFrame"] {
                border: 1px solid var(--border-subtle);
                border-radius: 22px;
                background: rgba(2,6,23,0.75);
            }
            div[data-testid="stAlert"] {
                border-radius: 18px;
                background: rgba(248,113,113,0.08);
                border: 1px solid rgba(248,113,113,0.35);
            }
            @media (max-width: 980px) {
                .hero-card {
                    flex-direction: column;
                    text-align: left;
                    padding: 2rem 1.8rem;
                    border-radius: 30px;
                }
                .hero-card h1 {
                    font-size: 1.8rem;
                }
                .hero-meta-value {
                    font-size: 1.2rem;
                }
                .hero-meta {
                    border-left: none;
                    border-top: 1px solid rgba(148,163,184,0.25);
                    padding-left: 0;
                    padding-top: 1.2rem;
                    text-align: left;
                    width: 100%;
                }
                .block-container {
                    padding: 1.8rem 1.6rem 2.2rem;
                    margin-top: 0;
                }
                .metrics-grid {
                    grid-template-columns: repeat(auto-fit, minmax(160px, 1fr));
                    gap: 12px;
                }
                .panel-heading strong {
                    font-size: 1rem;
                }
                .kpi-card {
                    padding: 1rem 1rem;
                }
                .kpi-value {
                    font-size: 1.6rem;
                }
                .kpi-meta small {
                    font-size: 0.7rem;
                }
                .viz-center {
                    min-height: 260px;
                }
                .viz-center div[data-testid="stPlotlyChart"],
                .viz-center div[data-testid="stVegaLiteChart"] {
                    min-height: 260px;
                }
                [data-baseweb="select"] > div {
                    min-height: 48px;
                    padding: 0 0.8rem !important;
                }
                .stButton > button {
                    height: 48px;
                    font-size: 0.92rem;
                }
                .kpi-summary-tag {
                    font-size: 0.72rem;
                }
                [data-testid="stHorizontalBlock"] {
                    flex-direction: column;
                    gap: 1rem;
                }
                [data-testid="column"] {
                    width: 100% !important;
                    padding-right: 0 !important;
                    padding-left: 0 !important;
                }
            }
            @media (max-width: 640px) {
                .hero-card {
                    padding: 1.5rem 1.2rem;
                    border-radius: 24px;
                }
                .hero-card h1 {
                    font-size: 1.4rem;
                }
                .hero-description {
                    font-size: 0.9rem;
                }
                .metrics-grid {
                    grid-template-columns: repeat(auto-fit, minmax(140px, 1fr));
                    gap: 12px;
                }
                .kpi-card {
                    padding: 0.9rem 0.9rem;
                }
                .kpi-label {
                    font-size: 0.7rem;
                }
                .kpi-value {
                    font-size: 1.3rem;
                }
                .stButton > button {
                    height: 44px;
                    font-size: 0.85rem;
                }
                .panel-heading {
                    font-size: 0.78rem;
                }
                .input-label {
                    font-size: 0.66rem;
                }
                .viz-center,
                .viz-center div[data-testid="stPlotlyChart"],
                .viz-center div[data-testid="stVegaLiteChart"] {
                    min-height: 220px;
                }
                .hero-meta {
                    padding-top: 0.8rem;
                }
                [data-testid="stHorizontalBlock"] {
                    gap: 0.8rem;
                }
                [data-testid="stHorizontalBlock"] > div [data-testid="column"] {
                    margin-bottom: 1rem;
                }
                [data-testid="stHorizontalBlock"] > div [data-testid="column"] > div {
                    justify-content: center;
                }
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
    # Menggunakan native container dengan border untuk membungkus upload
    with container.container(border=True):
        st.markdown("<div class='panel-heading'><strong>Upload Dataset</strong></div>", unsafe_allow_html=True)
        st.markdown(
            "<p class='panel-description'>Unggah file CSV atau XLSX langsung dari BSP (maksimal 25MB).</p>",
            unsafe_allow_html=True,
        )

        col1, col2 = st.columns([3, 1], gap="medium")
        with col1:
            st.markdown("<div class='input-label'>File Transaksi</div>", unsafe_allow_html=True)
            uploaded = st.file_uploader(
                "File",
                type=["csv", "xlsx"],
                key="top_uploader",
                label_visibility="collapsed",
            )
        with col2:
            st.markdown("<div class='input-label'>&nbsp;</div>", unsafe_allow_html=True)
            process = st.button("Proses & Simpan", use_container_width=True, type="primary")

    if not process:
        return

    if uploaded is None:
        st.toast("⚠️ Pilih file terlebih dahulu.", icon="⚠️")
        return

    try:
        with st.spinner("Memproses data transaksi..."):
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
        
        st.toast("✅ Laporan berhasil disimpan!", icon="✅")
        st.rerun()
        
    except Exception as exc: 
        st.error(f"Gagal memproses file: {exc}")


def main() -> None:
    # Header Section
    main_container = st.container()
    upload_controls(report_store, main_container)

    dataset_version = refresh_dataset_version()

    try:
        df, metadata_payload = load_cached_dataset(report_store, None, dataset_version)
        metadata = ReportMetadata.from_dict(metadata_payload) if metadata_payload else None
    except FileNotFoundError:
        st.info("👋 Selamat datang! Silakan unggah file pertama Anda di atas.")
        return

    df = annotate_period_columns(df)
    
    # Render Hero Header (Manual HTML di components)
    components.render_header(metadata)

    # --- FILTER & KPI SECTION ---
    # Menggunakan container(border=True) agar terbungkus rapi
    with st.container(border=True):
        st.markdown("<div class='panel-heading'><strong>Filter & Key Metrics</strong></div>", unsafe_allow_html=True)
        st.markdown(
            "<p class='panel-description'>Gunakan filter di bawah untuk menyesuaikan periode dan template sebelum melihat metrik utama.</p>",
            unsafe_allow_html=True,
        )
        
        period_options = get_period_options(df)
        
        # Baris Filter
        year_filter, month_filter, week_filter = components.render_period_selector(
            period_options.get("years", []), period_options.get("months_by_year", {})
        )
        period_filtered_df = apply_period_filters(df, year_filter, month_filter, week_filter)

        selectors = get_filter_options(period_filtered_df)
        
        col_tmpl, _ = st.columns([1, 2])
        with col_tmpl:
            template_filter = components.render_template_filter(selectors.get("templates", []))
        
        filtered_df = apply_filters(period_filtered_df, template=template_filter)
        
        if filtered_df.empty:
            st.warning("Filter tidak menemukan data. Menampilkan default.")
            filtered_df = period_filtered_df if not period_filtered_df.empty else df

        st.markdown("---") # Separator visual
        kpis = compute_kpis(filtered_df)
        components.render_kpi_cards(kpis)

    # --- CHARTS SECTION ---
    status_df = build_status_breakdown(filtered_df)
    category_df = category_breakdown(filtered_df)
    read_activity_df = read_vs_unread_by_hour(filtered_df)

    # Grid Layout untuk Chart: 2 Kolom
    col_left, col_right = st.columns(2)
    
    with col_left:
        st.markdown("<div class='panel-heading'><strong>Kategori Pesan</strong></div>", unsafe_allow_html=True)
        components.render_category_donut(category_df)

    with col_right:
        st.markdown("<div class='panel-heading'><strong>Status Pengiriman</strong></div>", unsafe_allow_html=True)
        components.render_status_comparison(status_df)

    st.markdown("<div style='margin-bottom:1.5rem;'></div>", unsafe_allow_html=True)

    # Full Width Charts
    st.markdown("<div class='panel-heading'><strong>Aktivitas Baca (Hourly)</strong></div>", unsafe_allow_html=True)
    components.render_read_activity(read_activity_df)



if __name__ == "__main__":
    main()
