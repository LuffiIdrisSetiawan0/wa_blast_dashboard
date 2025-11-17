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
<div class="hero-card">
    <div>
        <p class="hero-eyebrow">WhatsApp Insights</p>
        <h1>WA Campaign Performance</h1>
        <p class="hero-description">Pantau efektivitas blast marketing dan status pengiriman.</p>
    </div>
    <div class="hero-meta">
        <p class="hero-meta-label">Dataset Periode</p>
        <p class="hero-meta-value">{period_label}</p>
        <p class="hero-updated">Terakhir diperbarui {uploaded_info}</p>
    </div>
</div>
        """,
        unsafe_allow_html=True,
    )


def render_template_filter(templates: Iterable[str]) -> Optional[str]:
    choices = ["Semua template"] + sorted(set(filter(None, templates)))
    st.markdown("<div class='input-label'>Template</div>", unsafe_allow_html=True)
    selected = st.selectbox("", choices, label_visibility="collapsed")
    return None if selected == "Semua template" else selected


def render_kpi_cards(kpis: Dict[str, float]) -> None:
    total_messages = kpis.get("total", 0)
    total = total_messages or 1

    cards = [
        {
            "title": "Total Cost",
            "value": f"Rp {kpis.get('total_cost', 0):,.0f}",
            "meta": "Akumulasi Rate",
            "delta":"",
            "accent": "accent-cyan",
        },
        {
            "title": "Delivered",
            "value": f"{kpis.get('delivered', 0):,}",
            "meta": "Success Rate",
            "delta": f"{kpis.get('delivery_rate', 0) * 100:.1f}%",
            "accent": "accent-blue",
        },
        {
            "title": "Read",
            "value": f"{kpis.get('read', 0):,}",
            "meta": "Open Rate",
            "delta": f"{kpis.get('read_rate', 0) * 100:.1f}%",
            "accent": "accent-pink",
        },
        {
            "title": "Unread",
            "value": f"{kpis.get('unread', 0):,}",
            "meta": "Ignored",
            "delta": f"{(kpis.get('unread', 0) / total) * 100:.1f}%",
            "accent": "accent-amber",
        },
        {
            "title": "Failed",
            "value": f"{kpis.get('failed', 0):,}",
            "meta": "Undelivered",
            "delta": f"{(kpis.get('failed', 0) / total) * 100:.1f}%",
            "accent": "accent-slate",
        },
    ]

    summary_tag = f"<div class='kpi-summary-tag'>Total Pesan: {total_messages:,}</div>" if total_messages else ""
    cards_html = []
    for card in cards:
        delta_html = f"<span>{card['delta']}</span>" if card["delta"] else "<span>&nbsp;</span>"
        cards_html.append(
            "".join(
                [
                    f"<div class=\"kpi-card {card['accent']}\">",
                    f"<p class=\"kpi-label\">{card['title']}</p>",
                    f"<p class=\"kpi-value\">{card['value']}</p>",
                    "<div class=\"kpi-meta\">",
                    f"<small>{card['meta']}</small>",
                    delta_html,
                    "</div>",
                    "</div>",
                ]
            )
        )

    st.markdown(
        summary_tag + "<div class='metrics-grid'>" + "".join(cards_html) + "</div>",
        unsafe_allow_html=True,
    )


def render_category_donut(category_df: pd.DataFrame) -> None:
    if category_df.empty:
        st.markdown(
            "<p class='panel-description' style='text-align:center; padding:1.5rem;'>Belum ada data kategori untuk periode ini.</p>",
            unsafe_allow_html=True,
        )
        return
    
    total = category_df["total"].sum()
    fig = px.pie(
        category_df,
        values="total",
        names="category",
        hole=0.64,
        color_discrete_sequence=["#22d3ee", "#818cf8", "#f472b6", "#34d399", "#facc15"],
    )
    fig.update_layout(
        annotations=[
            {"text": f"<b>{total:,}</b><br>Pesan", "showarrow": False, "font": {"size": 15, "color": "#f8fafc"}}
        ],
        height=320,
        margin=dict(l=0, r=0, t=10, b=20),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        showlegend=True,
        legend=dict(
            orientation="h",
            yanchor="bottom",
            y=-0.25,
            xanchor="center",
            x=0.5,
            font=dict(color="#cbd5f5"),
        ),
        font=dict(color="#e2e8f0"),
    )
    legend_caption = "<div style='text-align:center; margin-top:0.6rem; color:#cbd5f5; font-size:0.85rem;'>"
    legend_caption += " | ".join(
        f"{row['category']}: <strong>{int(row['total']):,}</strong>" for _, row in category_df.iterrows()
    )
    legend_caption += "</div>"

    fig.update_traces(textinfo="none")
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})
    st.markdown(legend_caption, unsafe_allow_html=True)


def render_status_comparison(status_df: pd.DataFrame) -> None:
    if status_df.empty:
        st.markdown(
            "<p class='panel-description' style='text-align:center; padding:1.5rem;'>Belum ada data status yang siap ditampilkan.</p>",
            unsafe_allow_html=True,
        )
        return
        
    # 1. Buat Base Chart
    base = alt.Chart(status_df).encode(
        y=alt.Y("status:N", title=None, sort="-x", axis=alt.Axis(labelColor="#cbd5f5", tickSize=0))
    )

    # 2. Buat Bar Layer
    bars = base.mark_bar(cornerRadius=8, size=28).encode(
        x=alt.X("count:Q", title=None, axis=None),
        color=alt.Color(
            "status:N",
            legend=None,
            scale=alt.Scale(range=["#22d3ee", "#818cf8", "#f472b6", "#facc15", "#34d399", "#64748b"]),
        ),
        tooltip=["status", alt.Tooltip("count", format=",")]
    )
    
    # 3. Buat Text Layer
    text = base.mark_text(align='left', dx=6, color="#f8fafc").encode(
        x=alt.X("count:Q"),
        text=alt.Text("count:Q", format=",")
    )
    
    # 4. Gabungkan (Layering) TERLEBIH DAHULU, baru atur properti container
    final_chart = (
        (bars + text)
        .properties(height=300, background="transparent")
        .configure_view(strokeWidth=0)
        .configure_axis(grid=False, domainColor="rgba(148,163,184,0.3)", labelFont="Inter", titleFont="Inter")
    )
    
    st.altair_chart(final_chart, use_container_width=True)


def render_read_activity(hour_df: pd.DataFrame) -> None:
    if hour_df.empty or hour_df["read"].sum() == 0:
        st.markdown(
            "<p class='panel-description' style='text-align:center; padding:1.5rem;'>Belum ada data aktivitas baca untuk kombinasi filter ini.</p>",
            unsafe_allow_html=True,
        )
        return

    if "block" not in hour_df.columns:
        hour_df = hour_df.reset_index(drop=True)
        hour_df["block"] = hour_df.index * 3

    hour_df = hour_df.sort_values("block")
    categories = hour_df["label"].tolist()

    max_value = hour_df["read"].max()

    fig = px.area(
        hour_df,
        x="label",
        y="read",
        labels={"read": "Pesan Dibaca", "label": "Rentang Jam (WIB)"},
        color_discrete_sequence=["#22d3ee"],
        category_orders={"label": categories},
    )
    traces = fig.data
    if traces:
        marker_colors = ["#f97316"] * len(hour_df)
        max_index = hour_df["read"].idxmax()
        if pd.notna(max_index):
            max_position = hour_df.index.get_loc(max_index)
            marker_colors[max_position] = "#22c55e"
        traces[0].update(
            mode="lines+markers",
            line=dict(width=2.4),
            fill=None,
            marker=dict(size=12, color=marker_colors, line=dict(width=1.2, color="#0f172a")),
        )
    fig.update_layout(
        height=320,
        showlegend=False,
        margin=dict(l=0, r=0, t=10, b=10),
        paper_bgcolor="rgba(0,0,0,0)",
        plot_bgcolor="rgba(0,0,0,0)",
        font=dict(color="#e2e8f0"),
        xaxis=dict(showgrid=False, color="#94a3b8", type="category"),
        yaxis=dict(showgrid=True, gridcolor="rgba(241,245,249,0.18)", color="#94a3b8"),
    )
    st.plotly_chart(fig, use_container_width=True, config={"displayModeBar": False})


def render_cost_summary(category_df: pd.DataFrame) -> None:
    if category_df.empty:
        st.markdown(
            "<p class='panel-description' style='text-align:center; padding:1.2rem;'>Belum ada data biaya untuk filter yang dipilih.</p>",
            unsafe_allow_html=True,
        )
        return
    
    display_df = category_df.rename(
        columns={"category": "Kategori", "rate_sum": "Biaya (Rp)", "delivered": "Jumlah"}
    ).copy()
    
    styled = (
        display_df.style.format(
            {
                "Biaya (Rp)": lambda v: f"Rp {v:,.0f}".replace(",", "."),
                "Jumlah": lambda v: f"{int(v):,}".replace(",", "."),
            }
        )
        .set_table_styles(
            [
                {"selector": "th", "props": [("text-align", "center")]},
                {"selector": "td", "props": [("text-align", "center")]},
            ]
        )
        .hide(axis="index")
    )
    st.markdown(styled.to_html(), unsafe_allow_html=True)


def render_period_selector(years: Iterable[int], months_by_year: Dict[int, List[int]]):
    available_years = sorted(years) or [datetime.utcnow().year]
    months_map = months_by_year or {}
    
    if "period_year" not in st.session_state:
        st.session_state["period_year"] = available_years[-1]
    if "period_month" not in st.session_state:
        st.session_state["period_month"] = None
    if "period_week" not in st.session_state:
        st.session_state["period_week"] = None

    def reset_month_week() -> None:
        st.session_state["period_month"] = None
        st.session_state["period_week"] = None

    def reset_week() -> None:
        st.session_state["period_week"] = None

    col_year, col_month, col_week = st.columns(3)
    
    with col_year:
        # Pastikan index valid
        try:
            curr_idx = available_years.index(st.session_state["period_year"])
        except ValueError:
            curr_idx = 0
        st.markdown("<div class='input-label'>Tahun</div>", unsafe_allow_html=True)
        st.selectbox(
            "",
            available_years,
            index=curr_idx,
            key="period_year",
            on_change=reset_month_week,
            label_visibility="collapsed",
        )

    selected_year = st.session_state["period_year"]
    valid_months = months_map.get(selected_year, [])
    
    month_options = [None] + [m for m in range(1, 13) if m in valid_months]
    month_labels = {None: "Semua Bulan"}
    month_labels.update({m: calendar.month_name[m] for m in range(1, 13)})
    
    with col_month:
        current_month = st.session_state["period_month"]
        idx_month = month_options.index(current_month) if current_month in month_options else 0
        st.markdown("<div class='input-label'>Bulan</div>", unsafe_allow_html=True)
        st.selectbox(
            "",
            month_options,
            format_func=lambda value: month_labels.get(value, "Semua Bulan"),
            key="period_month",
            index=idx_month,
            on_change=reset_week,
            label_visibility="collapsed",
        )

    week_labels = {None: "Semua Minggu", 1: "Week 1", 2: "Week 2", 3: "Week 3", 4: "Week 4", 5: "Week 5"}
    with col_week:
        st.markdown("<div class='input-label'>Minggu</div>", unsafe_allow_html=True)
        st.selectbox(
            "",
            [None, 1, 2, 3, 4, 5],
            format_func=lambda value: week_labels.get(value),
            key="period_week",
            label_visibility="collapsed",
        )

    return st.session_state["period_year"], st.session_state["period_month"], st.session_state["period_week"]
