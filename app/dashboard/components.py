from __future__ import annotations

import calendar
from datetime import datetime
from typing import Dict, Iterable, List, Optional

import altair as alt
import pandas as pd
import plotly.express as px
import streamlit as st


def render_header(metadata) -> None:
    period_label = metadata.period_label if metadata else "-"
    uploaded_info = (
        metadata.uploaded_at.strftime("%d %b %Y %H:%M UTC") if metadata else datetime.utcnow().strftime("%d %b %Y %H:%M UTC")
    )
    st.markdown(
        f"""
        <div class="budget-hero">
            <div>
                <h1>WA Campaign Performance</h1>
                <p>Monitor KPI WhatsApp BSP layaknya dashboard keuangan elegan.</p>
            </div>
            <div class="hero-meta">
                <small>Periode aktif</small>
                <h3>{period_label}</h3>
                <small>Last Update: {uploaded_info}</small>
            </div>
        </div>
        """,
        unsafe_allow_html=True,
    )


def render_template_filter(templates: Iterable[str]) -> Optional[str]:
    choices = ["Semua template"] + sorted(set(templates))
    selected = st.selectbox("Filter Template", choices, label_visibility="collapsed")
    return None if selected == "Semua template" else selected


def render_kpi_cards(kpis: Dict[str, float]) -> None:
    cards = [
        ("Total Cost", f"Rp {kpis.get('total_cost', 0):,.0f}", "Akumulasi rate seluruh pesan"),
        ("Total Delivered", f"{kpis.get('delivered', 0):,}", f"{kpis.get('delivery_rate', 0)*100:.1f}% dari total"),
        ("Total Read", f"{kpis.get('read', 0):,}", f"{kpis.get('read_rate', 0)*100:.1f}% membaca"),
        ("Failed", f"{kpis.get('failed', 0):,}", "Pesan gagal / undelivered"),
    ]
    cols = st.columns(len(cards))
    for col, (title, value, desc) in zip(cols, cards):
        col.markdown(
            f"""
            <div class="budget-card">
                <p>{title}</p>
                <h2>{value}</h2>
                <span>{desc}</span>
            </div>
            """,
            unsafe_allow_html=True,
        )


def render_category_donut(category_df: pd.DataFrame) -> None:
    st.subheader("Perbandingan Category")
    if category_df.empty:
        st.info("Belum ada data category.")
        return
    total = category_df["total"].sum()
    fig = px.pie(
        category_df,
        values="total",
        names="category",
        hole=0.45,
        color_discrete_sequence=px.colors.sequential.Tealgrn,
    )
    fig.update_layout(
        annotations=[{"text": f"{total:,}<br>total", "showarrow": False, "font": {"size": 16}}],
        height=350,
        margin=dict(l=10, r=10, t=10, b=10),
        legend=dict(orientation="v"),
    )
    st.plotly_chart(fig, use_container_width=True)


def render_status_comparison(status_df: pd.DataFrame) -> None:
    st.subheader("Perbandingan Report Status")
    if status_df.empty:
        st.info("Belum ada data status.")
        return
    chart = (
        alt.Chart(status_df)
        .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
        .encode(
            x=alt.X("status:N", title="Status", sort="-y"),
            y=alt.Y("count:Q", title="Jumlah"),
            color=alt.Color("status:N", legend=None, scale=alt.Scale(scheme="teals")),
            tooltip=["status", "count"],
        )
    )
    st.altair_chart(chart, use_container_width=True)


def render_read_activity(hour_df: pd.DataFrame) -> None:
    st.subheader("Jam Audience Membaca vs Tidak Membaca")
    if hour_df.empty:
        st.info("Belum ada data jam baca.")
        return
    fig = px.line(
        hour_df,
        x="label",
        y=["read", "unread"],
        labels={"value": "Jumlah Pesan", "label": "Jam (UTC)", "variable": "Status"},
        markers=True,
    )
    fig.update_layout(height=350, legend=dict(orientation="h", y=-0.15), margin=dict(l=10, r=10, t=10, b=10))
    st.plotly_chart(fig, use_container_width=True)


def render_cost_summary(category_df: pd.DataFrame) -> None:
    st.subheader("Cost dari Total Rate per Category")
    if category_df.empty:
        st.info("Belum ada data biaya.")
        return
    display_df = category_df.rename(columns={"rate_sum": "Total Rate", "total": "Total Pesan"}).copy()
    display_df["Total Rate"] = display_df["Total Rate"].map(lambda x: f"Rp {x:,.0f}")
    st.dataframe(display_df[["category", "Total Pesan", "Total Rate"]], use_container_width=True, hide_index=True)


def render_period_selector(years: Iterable[int], months_by_year: Dict[int, List[int]]):
    st.markdown(
        """
        <div class="control-panel-heading">
            <span class="control-eyebrow">Filter Dataset</span>
            <h4>Filter Periode</h4>
            <p>Pilih tahun, bulan, dan minggu untuk mengisolasi performa kampanye.</p>
        </div>
        """,
        unsafe_allow_html=True,
    )
    available_years = sorted(years) or [datetime.utcnow().year]
    months_map = months_by_year or {}
    _init_period_state(available_years, months_map)

    def _label(text: str) -> None:
        st.markdown(f"<span class='period-filter-label'>{text}</span>", unsafe_allow_html=True)

    def _handle_year_change() -> None:
        st.session_state["period_month"] = None
        st.session_state["period_week"] = None

    def _handle_month_change() -> None:
        st.session_state["period_week"] = None

    year_index = available_years.index(st.session_state["period_year"]) if available_years else 0
    _label("Tahun")
    st.radio(
        "Tahun",
        available_years,
        index=year_index,
        key="period_year",
        horizontal=True,
        label_visibility="collapsed",
        on_change=_handle_year_change,
    )

    valid_months = months_map.get(st.session_state["period_year"], [])
    if st.session_state["period_month"] not in valid_months:
        st.session_state["period_month"] = None

    month_options: List[Optional[int]] = [None] + list(range(1, 13))
    month_labels = {None: "All"}
    month_labels.update({m: calendar.month_name[m] for m in range(1, 13)})
    month_value = st.session_state["period_month"] if st.session_state["period_month"] in month_options else None
    month_index = month_options.index(month_value)
    _label("Bulan")
    st.radio(
        "Bulan",
        month_options,
        index=month_index,
        key="period_month",
        format_func=lambda value: month_labels[value],
        horizontal=True,
        label_visibility="collapsed",
        on_change=_handle_month_change,
    )

    week_options: List[Optional[int]] = [None, 1, 2, 3, 4]
    week_labels = {None: "All", 1: "Week 1", 2: "Week 2", 3: "Week 3", 4: "Week 4"}
    week_value = st.session_state["period_week"] if st.session_state["period_week"] in week_options else None
    week_index = week_options.index(week_value)
    _label("Minggu")
    st.radio(
        "Minggu",
        week_options,
        index=week_index,
        key="period_week",
        format_func=lambda value: week_labels[value],
        horizontal=True,
        label_visibility="collapsed",
    )

    return st.session_state["period_year"], st.session_state["period_month"], st.session_state["period_week"]


def _init_period_state(years: List[int], months_by_year: Dict[int, List[int]]) -> None:
    if "period_year" not in st.session_state:
        st.session_state["period_year"] = years[-1] if years else datetime.utcnow().year
    if st.session_state["period_year"] not in years:
        st.session_state["period_year"] = years[-1] if years else datetime.utcnow().year
    if "period_month" not in st.session_state:
        st.session_state["period_month"] = None
    if "period_week" not in st.session_state:
        st.session_state["period_week"] = None
