"""
Santander Cycles — Analytics Dashboard
"""

import base64
import os
from pathlib import Path

import altair as alt
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
AXIS_FONT = "sans-serif"
AXIS_COLOR = "#6b7280"
AXIS_LABEL_SIZE = 13
AXIS_TITLE_SIZE = 16


@st.cache_data(ttl=3600, show_spinner=False)
def run_query(sql: str) -> pl.DataFrame:
    client = bigquery.Client(project=PROJECT_ID)
    return pl.from_arrow(client.query(sql).to_arrow())


# ── Data load (single query, ~230 rows) ───────────────────────────────────────
SQL = f"SELECT * FROM `{PROJECT_ID}.serve.dashboard`"

with st.spinner("Loading data…"):
    raw = run_query(SQL)

seasonal  = raw.filter(pl.col("tile") == "seasonal")
monthly   = raw.filter(pl.col("tile") == "monthly")
hourly_weekday = raw.filter(pl.col("tile") == "hourly_weekday").rename({"total_journeys": "rental_count"})

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

# ── Tile 1: When London rides ─────────────────────────────────────────────────
st.header("🚴 When does London ride? Hour × day-of-week heatmap")
st.markdown("<div style='height: 30px;'></div>", unsafe_allow_html=True)

source = hourly_weekday

# Size of the hexbins
size = 19
# Count of distinct x features
xFeaturesCount = 24
# Count of distinct y features
yFeaturesCount = 7

chart_padding = size * 0.15

CHART_MAX_WIDTH = round(size * xFeaturesCount * 2 * 1.19,0)

# the shape of a hexagon
hexagon = "M0,-2.3094010768L2,-1.1547005384 2,1.1547005384 0,2.3094010768 -2,1.1547005384 -2,-1.1547005384Z"

chart_hourly_weekday = alt.Chart(source).mark_point(size=size**2, shape=hexagon).encode(
    alt.X('xFeaturePos:Q')
        .title('Hour')
        .axis(
            grid=False,
            labelPadding=7,
            tickOpacity=0,
            domainOpacity=0,
            labels=True
            #values=list(range(25)),
            #format='d',
        ),
    alt.Y('day_name:N')
        .title('Weekday')
        .sort(alt.SortField('day_num'))
        .axis(labelPadding=25, tickOpacity=0, domainOpacity=0),
    stroke=alt.value('black'),
    strokeWidth=alt.value(0.2),
    fill=alt.Fill('sum(rental_count):Q')
        .title('Rentals')
        .scale(scheme='goldred'),
    tooltip=[
        alt.Tooltip('hour:Q', title='Hour'),
        alt.Tooltip('day_name:N', title='Day'),
        alt.Tooltip('sum(rental_count):Q', title='Rentals', format=',')
    ]
).transform_calculate(
    # This field is required for the hexagonal X-Offset
    xFeaturePos='(datum.day_num % 2) / 2 + datum.hour'
).properties(
    # Exact scaling factors to make the hexbins fit
    width=size * xFeaturesCount * 2 * 1.20, # 1.2x factor to fix Streamlit vs Marimo rendering
    height=size * yFeaturesCount * 1.7320508076 * 1.30,  # 1.7320508076 is approx. sin(60°)*2
    padding={
        "top": chart_padding # Adding padding to fix Streamlit cutting off top of hexagons
    },
).configure_axis(
    labelFont=AXIS_FONT,
    labelFontSize=AXIS_LABEL_SIZE,
    labelColor=AXIS_COLOR,
    titleFont=AXIS_FONT,
    titleFontSize=AXIS_TITLE_SIZE,
    titleFontWeight='normal',
    titleColor=AXIS_COLOR,
).configure_legend(
    offset=30,            # Distance from the right side of the hexes to the legend
    titlePadding=20,          # Space between "Rentals" and the bar
    labelAlign='left',        # Force alignment away from the bar
    labelOffset=15,       # Distance from the color bar to the text labels (e.g., 2,000,000)
    gradientLength=210,   # Makes the color bar taller to match your 2x chart height
    gradientThickness=20,
    titleFont=AXIS_FONT,
    titleFontSize=AXIS_TITLE_SIZE,
    titleFontWeight='normal',
    titleColor=AXIS_COLOR,
    labelFont=AXIS_FONT,
    labelFontSize=AXIS_LABEL_SIZE,
    labelColor=AXIS_COLOR,
    labelFontWeight='normal',
).configure_view(
    strokeWidth=0
)

st.altair_chart(chart_hourly_weekday, width="content", theme=None)

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
    yaxis=dict(title="Total journeys"),
    legend=dict(
        orientation="h",
        yanchor="bottom", y=1.02,
        xanchor="right",  x=1,
    ),
    plot_bgcolor="white",
    margin=dict(t=40, b=40),
    height=450,
)
fig2.update_xaxes(
    showgrid=False,
    showline=True,
    linewidth=1,
    linecolor="#888",
    title_font=dict(family=AXIS_FONT, size=AXIS_TITLE_SIZE, color=AXIS_COLOR),
    tickfont=dict(family=AXIS_FONT, size=AXIS_LABEL_SIZE, color=AXIS_COLOR),
)
fig2.update_yaxes(
    gridcolor="#ccc",
    gridwidth=1,
    showline=True,
    linewidth=1,
    linecolor="#888",
    title_font=dict(family=AXIS_FONT, size=AXIS_TITLE_SIZE, color=AXIS_COLOR),
    tickfont=dict(family=AXIS_FONT, size=AXIS_LABEL_SIZE, color=AXIS_COLOR),
)

fig2.update_layout(width=CHART_MAX_WIDTH)
st.plotly_chart(fig2, width="content")

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
    xaxis=dict(title="Year"),
    yaxis=dict(title="Total journeys"),
    plot_bgcolor="white",
    margin=dict(t=40, b=40),
    height=450,
    showlegend=False,
)
fig3.update_xaxes(
    showgrid=False,
    showline=True,
    linewidth=1,
    linecolor="#888",
    dtick="M12",
    tickformat="%Y",
    title_font=dict(family=AXIS_FONT, size=AXIS_TITLE_SIZE, color=AXIS_COLOR),
    tickfont=dict(family=AXIS_FONT, size=AXIS_LABEL_SIZE, color=AXIS_COLOR),
)
fig3.update_yaxes(
    gridcolor="#ccc",
    gridwidth=1,
    showline=True,
    linewidth=1,
    linecolor="#888",
    title_font=dict(family=AXIS_FONT, size=AXIS_TITLE_SIZE, color=AXIS_COLOR),
    tickfont=dict(family=AXIS_FONT, size=AXIS_LABEL_SIZE, color=AXIS_COLOR),
)

fig3.update_layout(width=CHART_MAX_WIDTH)
st.plotly_chart(fig3, width="content")
