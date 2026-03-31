"""
Santander Cycles — Analytics Dashboard
Run: make stream-dash
"""

import base64
import os
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import pandas as pd
import plotly.graph_objects as go
import streamlit as st
from google.cloud import bigquery

# ── Page config ────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Santander Cycles",
    page_icon="🚲",
    layout="wide",
)


PROJECT        = os.environ["GCP_PROJECT"]
MRT            = f"{PROJECT}.san_cycles_mrt"
SANTANDER_RED  = "#EC0000"


@st.cache_data(ttl=3600, show_spinner=False)
def run_query(sql: str) -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT)
    return client.query(sql).to_dataframe()


# ── SQL definitions ────────────────────────────────────────────────────────────
SQL_DATE_RANGE = f"""
    SELECT
        EXTRACT(YEAR FROM MIN(journey_month)) AS min_year,
        EXTRACT(YEAR FROM MAX(journey_month)) AS max_year
    FROM `{MRT}.kpis_monthly`
"""

SQL_BIKE_YEARLY = f"""
    SELECT
        EXTRACT(YEAR FROM journey_month)                                    AS year,
        bike_id,
        SUM(total_rides)                                                    AS total_rides,
        ROUND(SUM(total_duration_seconds) / 3600.0, 1)                     AS total_hours,
        ROUND(SUM(total_duration_seconds) / SUM(total_rides) / 60.0, 1)   AS avg_duration_minutes
    FROM `{MRT}.bike_stats`
    WHERE bike_id IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
"""

SQL_SEASONAL = f"""
    SELECT
        CAST(EXTRACT(YEAR FROM journey_month) AS INT64) AS year,
        CASE
            WHEN EXTRACT(MONTH FROM journey_month) IN (3, 4, 5)   THEN 'Spring'
            WHEN EXTRACT(MONTH FROM journey_month) IN (6, 7, 8)   THEN 'Summer'
            WHEN EXTRACT(MONTH FROM journey_month) IN (9, 10, 11) THEN 'Autumn'
            ELSE 'Winter'
        END AS season,
        SUM(total_journeys) AS trips
    FROM `{MRT}.kpis_monthly`
    GROUP BY 1, 2
    ORDER BY 1, 2
"""

# ── Parallel data load ─────────────────────────────────────────────────────────
with st.spinner("Loading data…"):
    with ThreadPoolExecutor(max_workers=3) as pool:
        f_date     = pool.submit(run_query, SQL_DATE_RANGE)
        f_bike     = pool.submit(run_query, SQL_BIKE_YEARLY)
        f_seasonal = pool.submit(run_query, SQL_SEASONAL)
    date_range  = f_date.result()
    bike_yearly = f_bike.result()
    seasonal    = f_seasonal.result()

# ── Header ─────────────────────────────────────────────────────────────────────
logo_path = Path(__file__).parent / "assets" / "santander_logo.svg"
svg_b64   = base64.b64encode(logo_path.read_bytes()).decode()
logo_html = (
    f'<img src="data:image/svg+xml;base64,{svg_b64}" '
    f'style="height:0.85em;vertical-align:middle;margin-right:8px;margin-bottom:3px;">'
)

min_year = int(date_range["min_year"].iloc[0])
max_year = int(date_range["max_year"].iloc[0])

st.markdown(f"""
<div style="display:flex; justify-content:space-between; align-items:baseline;">
  <h1 style="color:{SANTANDER_RED}; margin:0;">{logo_html}Santander Cycles</h1>
  <span style="color:#888; font-size:0.9rem;">TfL journey data · {min_year}–{max_year}</span>
</div>
""", unsafe_allow_html=True)
st.divider()

# ── Tile 1: Most loved bike of the year ───────────────────────────────────────
years    = sorted(bike_yearly["year"].dropna().unique().astype(int), reverse=True)

col_title, col_select = st.columns([7, 2])
with col_title:
    st.header("🏆 Most loved bike of the year")
with col_select:
    st.markdown("<br>", unsafe_allow_html=True)
    selected_year = st.selectbox("", years, index=0, label_visibility="collapsed")

year_df  = bike_yearly[bike_yearly["year"] == selected_year].reset_index(drop=True)
top_bike = year_df.iloc[0]

col1, col2, col3 = st.columns(3)
col1.metric("Bike ID",         f"#{int(top_bike['bike_id'])}")
col2.metric("Total rides",     f"{int(top_bike['total_rides']):,}")
col3.metric("Hours in saddle", f"{top_bike['total_hours']:,.1f}")

st.divider()

# ── Tile 2: Rides by season & year ────────────────────────────────────────────
st.header("🌦️ Rides by season")

SEASON_ORDER  = ["Winter", "Autumn", "Summer", "Spring"]
SEASON_COLORS = {
    "Winter": "#4E8FD4",
    "Autumn": "#E07B39",
    "Summer": "#F5C842",
    "Spring": "#6ABF69",
}

fig2 = go.Figure()
for season in SEASON_ORDER:
    df_s = seasonal[seasonal["season"] == season].sort_values("year")
    fig2.add_trace(go.Bar(
        x=df_s["year"],
        y=df_s["trips"],
        name=season,
        marker_color=SEASON_COLORS[season],
    ))

fig2.update_layout(
    barmode="stack",
    xaxis=dict(title="Year", tickmode="linear", dtick=1),
    yaxis=dict(title="Number of trips"),
    legend=dict(
        traceorder="reversed",
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="right",  x=1,
    ),
    plot_bgcolor="white",
    margin=dict(t=40, b=40),
    height=450,
)
fig2.update_xaxes(showgrid=False)
fig2.update_yaxes(gridcolor="#eeeeee")

st.plotly_chart(fig2, use_container_width=True)
