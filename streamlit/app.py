"""
Santander Cycles — Analytics Dashboard
=======================================
Run: uv run streamlit run dashboard/app.py
"""

import os
import streamlit as st
import plotly.express as px
from google.cloud import bigquery

# ── Page config ───────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Santander Cycles Analytics",
    page_icon="🚲",
    layout="wide",
)

# ── BigQuery client ───────────────────────────────────────────────────────────
PROJECT  = os.environ["GCP_PROJECT"]
DATASET  = "santander_cycles_mart"


@st.cache_data(ttl=3600)
def query(sql: str) -> list[dict]:
    """Run a BigQuery query and return rows as a list of dicts. Cached 1 hour."""
    client = bigquery.Client(project=PROJECT)
    return [dict(r) for r in client.query(sql).result()]


def tbl(name: str) -> str:
    return f"`{PROJECT}.{DATASET}.{name}`"


# ── Header ────────────────────────────────────────────────────────────────────
st.title("🚲 Santander Cycles Analytics")
st.caption("Data: TfL Santander Cycles usage statistics")
st.divider()

# ── Yearly overview ───────────────────────────────────────────────────────────
st.header("Rides over time")

yearly = query(f"""
    SELECT
        year,
        total_rides,
        unique_bikes,
        active_stations,
        round(total_hours, 0) as total_hours,
        round(avg_duration_minutes, 1) as avg_duration_minutes,
        round(longest_ride.duration_seconds / 3600.0, 2) as longest_ride_hours,
        longest_ride.start_station_name as longest_ride_from,
        longest_ride.end_station_name as longest_ride_to,
        longest_ride.bike_id as longest_ride_bike
    FROM {tbl("yearly_stats")}
    ORDER BY year
""")

if yearly:
    col1, col2, col3, col4 = st.columns(4)
    latest = yearly[-1]
    col1.metric("Total rides",    f"{latest['total_rides']:,}")
    col2.metric("Total hours",    f"{latest['total_hours']:,.0f}")
    col3.metric("Unique bikes",   f"{latest['unique_bikes']:,}")
    col4.metric("Active stations", f"{latest['active_stations']:,}")

    fig = px.bar(
        yearly,
        x="year",
        y="total_rides",
        title="Total rides per year",
        labels={"year": "Year", "total_rides": "Rides"},
        color_discrete_sequence=["#e63946"],
    )
    fig.update_layout(showlegend=False)
    st.plotly_chart(fig, use_container_width=True)

st.divider()

# ── Ride of the year ──────────────────────────────────────────────────────────
st.header("🏆 Ride of the year")
st.caption("Longest single ride each year")

ride_of_year = query(f"""
    SELECT
        year,
        longest_ride.rental_id as rental_id,
        longest_ride.bike_id as bike_id,
        longest_ride.start_station_name as from_station,
        longest_ride.end_station_name as to_station,
        round(longest_ride.duration_seconds / 3600.0, 2) as duration_hours
    FROM {tbl("yearly_stats")}
    ORDER BY year
""")

if ride_of_year:
    st.dataframe(
        ride_of_year,
        column_config={
            "year":           st.column_config.NumberColumn("Year",         format="%d"),
            "rental_id":      st.column_config.TextColumn("Rental ID"),
            "bike_id":        st.column_config.TextColumn("Bike"),
            "from_station":   st.column_config.TextColumn("From"),
            "to_station":     st.column_config.TextColumn("To"),
            "duration_hours": st.column_config.NumberColumn("Duration (hrs)", format="%.2f"),
        },
        hide_index=True,
        use_container_width=True,
    )

st.divider()

# ── Bicycle of the year ───────────────────────────────────────────────────────
st.header("🚴 Bicycle of the year")

bikes = query(f"""
    SELECT
        bike_id,
        total_rides,
        round(total_hours, 1) as total_hours,
        round(avg_duration_minutes, 1) as avg_duration_minutes
    FROM {tbl("bike_stats")}
    ORDER BY total_hours DESC
    LIMIT 20
""")

if bikes:
    top_bike = bikes[0]
    col1, col2, col3 = st.columns(3)
    col1.metric("Hardest working bike",  f"#{top_bike['bike_id']}")
    col2.metric("Total hours in saddle", f"{top_bike['total_hours']:,.1f}")
    col3.metric("Total rides",           f"{top_bike['total_rides']:,}")

    fig = px.bar(
        bikes,
        x="bike_id",
        y="total_hours",
        title="Top 20 bikes by total hours",
        labels={"bike_id": "Bike ID", "total_hours": "Total hours"},
        color_discrete_sequence=["#457b9d"],
    )
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Full bicycle table"):
        st.dataframe(bikes, hide_index=True, use_container_width=True)

st.divider()

# ── Location of the year ──────────────────────────────────────────────────────
st.header("📍 Busiest stations")

stations = query(f"""
    SELECT
        station_name,
        total_departures,
        total_arrivals,
        total_activity,
        net_flow
    FROM {tbl("station_stats")}
    ORDER BY total_activity DESC
    LIMIT 20
""")

if stations:
    top_station = stations[0]
    col1, col2, col3 = st.columns(3)
    col1.metric("Busiest station",   top_station["station_name"])
    col2.metric("Total departures",  f"{top_station['total_departures']:,}")
    col3.metric("Total arrivals",    f"{top_station['total_arrivals']:,}")

    fig = px.bar(
        stations,
        x="total_activity",
        y="station_name",
        orientation="h",
        title="Top 20 stations by total activity",
        labels={"total_activity": "Total activity", "station_name": "Station"},
        color_discrete_sequence=["#2a9d8f"],
    )
    fig.update_layout(yaxis={"categoryorder": "total ascending"})
    st.plotly_chart(fig, use_container_width=True)

    with st.expander("Full station table"):
        st.dataframe(stations, hide_index=True, use_container_width=True)

st.divider()

# ── Ride hours per year ───────────────────────────────────────────────────────
st.header("⏱ Ride hours per year")

if yearly:
    fig = px.bar(
        yearly,
        x="year",
        y="total_hours",
        title="Total riding hours per year",
        labels={"year": "Year", "total_hours": "Hours"},
        color_discrete_sequence=["#f4a261"],
    )
    st.plotly_chart(fig, use_container_width=True)
