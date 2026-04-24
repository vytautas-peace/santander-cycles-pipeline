"""
Santander Cycles — Analytics Dashboard
"""

import base64
import os
from pathlib import Path

import polars as pl
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Santander Cycles",
    page_icon="🚲",
    layout="wide",
)

PROJECT_ID = os.environ["GCLOUD_PROJECT"]
SANTANDER_RED = "#EC0000"


@st.cache_data(ttl=3600, show_spinner=False)
def run_query(sql: str) -> pl.DataFrame:
    client = bigquery.Client(project=PROJECT_ID)
    return pl.from_arrow(client.query(sql).to_arrow())


# ── Data load (single query, ~230 rows) ───────────────────────────────────────
SQL = f"SELECT * FROM `{PROJECT_ID}.serve.dashboard`"

with st.spinner("Loading data…"):
    raw = run_query(SQL)

top_bikes = raw.filter(pl.col("tile") == "top_bike")
seasonal  = raw.filter(pl.col("tile") == "seasonal")
monthly   = raw.filter(pl.col("tile") == "monthly")

# ── Header ─────────────────────────────────────────────────────────────────────
logo_path = Path(__file__).parent / "santander_logo.svg"
svg_b64   = base64.b64encode(logo_path.read_bytes()).decode()
logo_html = (
    f'<img src="data:image/svg+xml;base64,{svg_b64}" '
    f'style="height:0.85em;vertical-align:middle;margin-right:8px;margin-bottom:3px;">'
)

min_year = monthly["journey_month"].min().year
max_year = monthly["journey_month"].max().year

st.markdown(f"""
<div style="display:flex; justify-content:space-between; align-items:baseline;">
  <h1 style="color:{SANTANDER_RED}; margin:0;">{logo_html}Santander Cycles</h1>
  <span style="color:#888; font-size:0.9rem;">TfL journey data · {min_year}–{max_year}</span>
</div>
""", unsafe_allow_html=True)
st.divider()

# ── Tile 1: Most loved bike of the year ───────────────────────────────────────
years    = top_bikes["year"].drop_nulls().unique().sort(descending=True).to_list()

col_title, col_select = st.columns([7, 2])
with col_title:
    st.header("🏆 Most loved bike of the year")
with col_select:
    st.markdown("<br>", unsafe_allow_html=True)
    selected_year = st.selectbox("Year", years, index=0, label_visibility="collapsed")

top_bike = top_bikes.filter(pl.col("year") == selected_year).row(0, named=True)

col1, col2, col3 = st.columns(3)
col1.metric("Bike ID",         f"#{top_bike['bike_id']}")
col2.metric("Total journeys",     f"{top_bike['total_journeys']:,}")
col3.metric("Hours in saddle", f"{top_bike['total_hours']:,.1f}")

st.divider()

# ── Tile 2: Rides by season & year ────────────────────────────────────────────
st.header("🌦️ Rides by season")

SEASON_ORDER  = ["Spring", "Summer", "Autumn", "Winter"]
SEASON_COLORS = {
    "Winter": "#4E8FD4",
    "Autumn": "#E07B39",
    "Summer": "#F5C842",
    "Spring": "#6ABF69",
}

fig2 = go.Figure()
for season in SEASON_ORDER:
    df_s = seasonal.filter(pl.col("season") == season).sort("year")
    fig2.add_trace(go.Bar(
        x=df_s["year"],
        y=df_s["total_journeys"],
        name=season,
        marker_color=SEASON_COLORS[season],
    ))

fig2.update_layout(
    barmode="group",
    xaxis=dict(title="Year", tickmode="linear", dtick=1),
    yaxis=dict(title="Number of trips"),
    legend=dict(
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="right",  x=1,
    ),
    plot_bgcolor="white",
    margin=dict(t=40, b=40),
    height=450,
)
fig2.update_xaxes(showgrid=False, showline=True, linewidth=1, linecolor="#888")
fig2.update_yaxes(gridcolor="#ccc", gridwidth=1, showline=True, linewidth=1, linecolor="#888")

st.plotly_chart(fig2, width='stretch')

st.divider()

# ── Tile 3: Total journeys by month ──────────────────────────────────────────
st.header("📈 Total journeys by month")

monthly_sorted = monthly.sort("journey_month")

fig3 = go.Figure()
fig3.add_trace(go.Scatter(
    x=monthly_sorted["journey_month"],
    y=monthly_sorted["total_journeys"],
    mode="lines",
    line=dict(color=SANTANDER_RED, width=1.5),
    hovertemplate="%{x|%b %Y}: %{y:,.0f} journeys<extra></extra>",
))

fig3.update_layout(
    xaxis=dict(title="Month"),
    yaxis=dict(title="Total journeys"),
    plot_bgcolor="white",
    margin=dict(t=40, b=40),
    height=450,
    showlegend=False,
)
fig3.update_xaxes(showgrid=False, showline=True, linewidth=1, linecolor="#888")
fig3.update_yaxes(gridcolor="#ccc", gridwidth=1, showline=True, linewidth=1, linecolor="#888")

st.plotly_chart(fig3, width='stretch')
