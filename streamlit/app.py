"""
Santander Cycles — Analytics Dashboard
Run: make stream-dash
"""

import os
from pathlib import Path

import pandas as pd
import plotly.express as px
import streamlit as st
from google.cloud import bigquery

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Santander Cycles",
    page_icon="🚲",
    layout="wide",
)

PROJECT = os.environ["GCP_PROJECT"]
MRT = f"{PROJECT}.san_cycles_mrt"


@st.cache_data(ttl=3600)
def run_query(sql: str) -> pd.DataFrame:
    client = bigquery.Client(project=PROJECT)
    return client.query(sql).to_dataframe()


# ── Header ────────────────────────────────────────────────────────────────────
logo_path = Path(__file__).parent / "assets" / "santander_logo.png"

if logo_path.exists():
    st.image(str(logo_path), width=200)
else:
    st.markdown("### Santander Cycles")

date_range = run_query(f"""
    SELECT
        EXTRACT(YEAR FROM MIN(journey_month)) AS min_year,
        EXTRACT(YEAR FROM MAX(journey_month)) AS max_year
    FROM `{MRT}.kpis_monthly`
""")
min_year = int(date_range["min_year"].iloc[0])
max_year = int(date_range["max_year"].iloc[0])

st.caption(f"TfL journey data · {min_year}–{max_year}")
st.divider()

# ── Tile 1: Most loved bike of the year ───────────────────────────────────────
bike_yearly = run_query(f"""
    SELECT
        EXTRACT(YEAR FROM journey_month)                                AS year,
        bike_id,
        SUM(total_rides)                                                AS total_rides,
        ROUND(SUM(total_duration_seconds) / 3600.0, 1)                 AS total_hours,
        ROUND(SUM(total_duration_seconds) / SUM(total_rides) / 60.0, 1)
                                                                        AS avg_duration_minutes
    FROM `{MRT}.bike_stats`
    WHERE bike_id IS NOT NULL
    GROUP BY 1, 2
    ORDER BY 1 DESC, 3 DESC
""")

years = sorted(bike_yearly["year"].dropna().unique().astype(int), reverse=True)

# Title + year selector inline
col_title, _, col_select = st.columns([5, 1, 2])
with col_title:
    st.header("🏆 Most loved bike of the year")
with col_select:
    st.markdown("<br>", unsafe_allow_html=True)
    selected_year = st.selectbox("", years, index=0, label_visibility="collapsed")

year_df = bike_yearly[bike_yearly["year"] == selected_year].reset_index(drop=True)
top_bike = year_df.iloc[0]
top10    = year_df.head(10)

col1, col2, col3 = st.columns(3)
col1.metric("Bike ID",          f"#{int(top_bike['bike_id'])}")
col2.metric("Total rides",      f"{int(top_bike['total_rides']):,}")
col3.metric("Hours in saddle",  f"{top_bike['total_hours']:,.1f}")

