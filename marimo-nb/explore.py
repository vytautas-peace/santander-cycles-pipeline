import marimo

__generated_with = "0.23.3"
app = marimo.App(width="full")


@app.cell
def _():
    import os
    import sqlalchemy
    import marimo as mo
    import altair as alt
    alt.data_transformers.enable("vegafusion")
    import polars as pl

    _project = os.environ.get("GCLOUD_PROJECT")
    engine = sqlalchemy.create_engine(f"bigquery://{_project}/serve")
    return alt, engine, mo, pl


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    # 🚲 Santander Cycles — Creative Data Explorations
    **137M journeys · 2012–2025 · London**

    Ten angles on the data — from hourly heatmaps to e-bike adoption curves.
    Each cell is self-contained: SQL → DataFrame → chart.
    """)
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 1 · When does London ride? — Hour × Day-of-Week heatmap
    """)
    return


@app.cell(hide_code=True)
def _(engine, mo):
    heatmap_df = mo.sql(
        f"""
        SELECT 
            EXTRACT(HOUR FROM start_datetime) as hour,
            EXTRACT(DAYOFWEEK FROM start_datetime) as weekday,
            COUNT(*) as journeys
        FROM `serve.fct_journeys`
        GROUP BY 1, 2
        """,
        engine=engine
    )
    return (heatmap_df,)


@app.cell
def _(alt, heatmap_df):
    _day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    chart_heatmap = (
        alt.Chart(heatmap_df)
        .mark_rect(stroke="white", strokeWidth=0.5)
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("weekday:O", sort=_day_order, title=None),
            color=alt.Color("journeys:Q", scale=alt.Scale(scheme="lightmulti"), title="Journeys"),
            tooltip=["weekday", "hour", "journeys"],
        )
        .properties(title="Journey count by hour & weekday (all time)", width=700, height=250)
    )
    chart_heatmap
    return


@app.cell(hide_code=True)
def _(engine, mo):
    hexbin_df = mo.sql(
        f"""
        SELECT 
            EXTRACT(HOUR FROM start_datetime) as hour,
            EXTRACT(MONTH FROM start_datetime) as month,
            EXTRACT(DAYOFWEEK FROM start_datetime) as day_num,
            ANY_VALUE(FORMAT_DATE('%a', start_datetime)) as day_name,
            COUNT(*) as rental_count
        FROM `serve.fct_journeys`
        GROUP BY 1,2,3
        """,
        engine=engine
    )
    return (hexbin_df,)


@app.cell
def _(alt, hexbin_df):
    source = hexbin_df

    # Size of the hexbins
    size = 20
    # Count of distinct x features
    xFeaturesCount = 12
    # Count of distinct y features
    yFeaturesCount = 7


    # the shape of a hexagon
    hexagon = "M0,-2.3094010768L2,-1.1547005384 2,1.1547005384 0,2.3094010768 -2,1.1547005384 -2,-1.1547005384Z"

    alt.Chart(source).mark_point(size=size**2, shape=hexagon).encode(
        alt.X('xFeaturePos:Q')
            .title('Month')
            .scale(zero=False)
            .axis(grid=False, labelPadding=7, tickOpacity=0, domainOpacity=0, labels=True, values=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12]),
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
            alt.Tooltip('month:Q', title='Month'),
            alt.Tooltip('day_name:N', title='Day'),
            alt.Tooltip('sum(rental_count):Q', title='Rentals', format=',')
        ]
    ).transform_calculate(
        # This field is required for the hexagonal X-Offset
        xFeaturePos='(datum.day_num % 2) / 2 + datum.month'
    ).properties(
        # Exact scaling factors to make the hexbins fit
        width=size * xFeaturesCount * 2,
        height=size * yFeaturesCount * 1.7320508076,  # 1.7320508076 is approx. sin(60°)*2
    ).configure_axis(
        labelFontSize=14,    # Increases the axis labels (e.g., "Mon", "Tue", "1", "2")
        titleFontSize=16     # Increases the axis titles (e.g., "Month", "Weekday")
    ).configure_legend(
            offset=30,            # Distance from the right side of the hexes to the legend
            titlePadding=20,          # Space between "Rentals" and the bar
            labelAlign='left',        # Force alignment away from the bar
            labelOffset=15,       # Distance from the color bar to the text labels (e.g., 2,000,000)
            gradientLength=210,   # Makes the color bar taller to match your 2x chart height
            gradientThickness=20,
            titleFontSize=16,    # Increases the legend title ("Rentals")
            labelFontSize=14
    ).configure_view(
        strokeWidth=0
    )
    return


@app.cell
def _(alt, hexbin_df):
    def _():
        source = hexbin_df

        # Size of the hexbins
        size = 20
        # Count of distinct x features
        xFeaturesCount = 24
        # Count of distinct y features
        yFeaturesCount = 7


        # the shape of a hexagon
        hexagon = "M0,-2.3094010768L2,-1.1547005384 2,1.1547005384 0,2.3094010768 -2,1.1547005384 -2,-1.1547005384Z"

        return alt.Chart(source).mark_point(size=size**2, shape=hexagon).encode(
            alt.X('xFeaturePos:Q')
                .title('Hour')
                .axis(
                    grid=False,
                    labelPadding=7,
                    tickOpacity=0,
                    domainOpacity=0,
                    labels=True
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
            width=size * xFeaturesCount * 2,
            height=size * yFeaturesCount * 1.7320508076,  # 1.7320508076 is approx. sin(60°)*2
        ).configure_axis(
            labelFontSize=14,    # Increases the axis labels (e.g., "Mon", "Tue", "1", "2")
            titleFontSize=16     # Increases the axis titles (e.g., "Month", "Weekday")
        ).configure_legend(
            offset=30,            # Distance from the right side of the hexes to the legend
            titlePadding=20,          # Space between "Rentals" and the bar
            labelAlign='left',        # Force alignment away from the bar
            labelOffset=15,       # Distance from the color bar to the text labels (e.g., 2,000,000)
            gradientLength=210,   # Makes the color bar taller to match your 2x chart height
            gradientThickness=20,
            titleFontSize=16,    # Increases the legend title ("Rentals")
            labelFontSize=14
        ).configure_view(
            strokeWidth=0
        )


    _()
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 2 · Monthly journey volumes by bike model — since model data began (2022)
    """)
    return


@app.cell
def _(engine, mo):
    annual_df = mo.sql(
        f"""
        SELECT
            DATE_TRUNC(DATE(start_datetime), MONTH) AS month,
            COALESCE(bike_model, 'UNKNOWN')         AS bike_model,
            COUNT(*)                                AS journeys
        FROM `serve.fct_journeys`
        WHERE start_datetime >= '2022-09-01'
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        engine=engine
    )
    return (annual_df,)


@app.cell
def _(alt, annual_df):
    chart_annual = (
        alt.Chart(annual_df)
        .mark_bar()
        .encode(
            x=alt.X("month:T", title="Month"),
            y=alt.Y("journeys:Q", stack="normalize", title="Share of journeys", axis=alt.Axis(format="%")),
            color=alt.Color(
                "bike_model:N",
                scale=alt.Scale(
                    domain=["CLASSIC", "PBSC_EBIKE", "UNKNOWN"],
                    range=["#e06c00", "#00b4d8", "#aaa"],
                ),
                title="Bike model",
            ),
            tooltip=["month", "bike_model", "journeys"],
        )
        .properties(title="Monthly journey mix by bike model (normalised, 2022+)", width=700, height=300)
    )
    chart_annual
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 3 · E-bike adoption curve — monthly share since 2022
    """)
    return


@app.cell
def _(engine, mo):
    ebike_df = mo.sql(
        f"""
        SELECT
            DATE_TRUNC(DATE(start_datetime), MONTH)    AS month,
            COUNTIF(bike_model = 'PBSC_EBIKE')         AS ebike_journeys,
            COUNT(*)                                   AS total_journeys,
            ROUND(
                COUNTIF(bike_model = 'PBSC_EBIKE') * 100.0 / COUNT(*), 2
            )                                          AS ebike_pct
        FROM `serve.fct_journeys`
        WHERE start_datetime >= '2022-01-01'
          AND bike_model IS NOT NULL
        GROUP BY 1
        ORDER BY 1
        """,
        engine=engine
    )
    return (ebike_df,)


@app.cell
def _(alt, ebike_df):
    _base = alt.Chart(ebike_df).encode(x=alt.X("month:T", title="Month"))
    _area = _base.mark_area(opacity=0.3, color="#00b4d8").encode(
        y=alt.Y("ebike_pct:Q", title="E-bike share (%)")
    )
    _line = _base.mark_line(color="#00b4d8", strokeWidth=2).encode(
        y="ebike_pct:Q",
        tooltip=["month", "ebike_pct", "ebike_journeys", "total_journeys"],
    )
    chart_ebike = (_area + _line).properties(
        title="Monthly e-bike share of journeys (% of known-model rides)", width=700, height=280
    )
    chart_ebike
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 4 · Top 20 busiest start stations (all time)
    """)
    return


@app.cell
def _(engine, mo):
    stations_df = mo.sql(
        f"""
        SELECT
            start_station_name AS station,
            COUNT(*)           AS departures
        FROM `serve.fct_journeys`
        WHERE start_station_name IS NOT NULL
        GROUP BY 1
        ORDER BY 2 DESC
        LIMIT 20
        """,
        engine=engine
    )
    return (stations_df,)


@app.cell
def _(alt, stations_df):
    chart_stations = (
        alt.Chart(stations_df)
        .mark_bar(color="#e06c00")
        .encode(
            x=alt.X("departures:Q", title="Total departures"),
            y=alt.Y("station:N", sort="-x", title=None),
            tooltip=["station", "departures"],
        )
        .properties(title="Top 20 start stations by departure count", width=600, height=400)
    )
    chart_stations
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 5 · Weekday vs Weekend — average journeys per hour of day
    """)
    return


@app.cell
def _(engine, mo):
    wdwe_df = mo.sql(
        f"""
        SELECT
            EXTRACT(HOUR FROM start_datetime)          AS hour,
            IF(EXTRACT(DAYOFWEEK FROM start_datetime) IN (1,7), 'Weekend', 'Weekday') AS day_type,
            COUNT(*) / COUNT(DISTINCT DATE(start_datetime)) AS avg_journeys_per_day
        FROM `serve.fct_journeys`
        GROUP BY 1, 2
        ORDER BY 1
        """,
        engine=engine
    )
    return (wdwe_df,)


@app.cell
def _(alt, wdwe_df):
    chart_wdwe = (
        alt.Chart(wdwe_df)
        .mark_bar()
        .encode(
            x=alt.X("hour:O", title="Hour of day"),
            y=alt.Y("avg_journeys_per_day:Q", title="Avg journeys / day"),
            color=alt.Color(
                "day_type:N",
                scale=alt.Scale(domain=["Weekday", "Weekend"], range=["#e06c00", "#00b4d8"]),
                title=None,
            ),
            xOffset="day_type:N",
            tooltip=["hour", "day_type", alt.Tooltip("avg_journeys_per_day:Q", format=".0f")],
        )
        .properties(title="Average journeys per hour — Weekday vs Weekend", width=700, height=300)
    )
    chart_wdwe
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 6 · Has ride duration changed over 13 years? — Annual percentiles
    """)
    return


@app.cell
def _(engine, mo):
    duration_yr_df = mo.sql(
        f"""
        SELECT
            EXTRACT(YEAR FROM start_datetime)                           AS year,
            ROUND(APPROX_QUANTILES(duration_s/60, 100)[OFFSET(25)], 1) AS p25_min,
            ROUND(APPROX_QUANTILES(duration_s/60, 100)[OFFSET(50)], 1) AS median_min,
            ROUND(APPROX_QUANTILES(duration_s/60, 100)[OFFSET(75)], 1) AS p75_min,
            ROUND(APPROX_QUANTILES(duration_s/60, 100)[OFFSET(90)], 1) AS p90_min
        FROM `serve.fct_journeys`
        WHERE duration_s BETWEEN 60 AND 7200
        GROUP BY 1
        ORDER BY 1
        """,
        engine=engine
    )
    return (duration_yr_df,)


@app.cell
def _(alt, duration_yr_df):
    _melted = duration_yr_df.unpivot(
        on=["p25_min", "median_min", "p75_min", "p90_min"],
        index="year",
        variable_name="percentile",
        value_name="minutes",
    )
    chart_duration = (
        alt.Chart(_melted)
        .mark_line(point=True)
        .encode(
            x=alt.X("year:O", title="Year"),
            y=alt.Y("minutes:Q", title="Journey duration (min)"),
            color=alt.Color(
                "percentile:N",
                scale=alt.Scale(
                    domain=["p25_min", "median_min", "p75_min", "p90_min"],
                    range=["#f4a460", "#00b4d8", "#e06c00", "#8b2500"],
                ),
                title="Percentile",
            ),
            tooltip=["year", "percentile", "minutes"],
        )
        .properties(title="Journey duration percentiles by year (60s–2hr rides only)", width=700, height=300)
    )
    chart_duration
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 7 · Seasonality heatmap — journeys by month × year
    """)
    return


@app.cell
def _(engine, mo):
    seasonal_df = mo.sql(
        f"""
        SELECT
            EXTRACT(YEAR  FROM start_datetime) AS year,
            EXTRACT(MONTH FROM start_datetime) AS month,
            COUNT(*)                           AS journeys
        FROM `serve.fct_journeys`
        GROUP BY 1, 2
        ORDER BY 1, 2
        """,
        engine=engine
    )
    return (seasonal_df,)


@app.cell(hide_code=True)
def _(alt, pl, seasonal_df):
    _month_labels = {1: "Jan", 2: "Feb", 3: "Mar", 4: "Apr", 5: "May", 6: "Jun",
                     7: "Jul", 8: "Aug", 9: "Sep", 10: "Oct", 11: "Nov", 12: "Dec"}
    _df = seasonal_df.with_columns(
        pl.col("month")
        .cast(pl.Int32)
        .cast(pl.Utf8)
        .replace({str(k): v for k, v in _month_labels.items()})
        .alias("month_name")
    )
    chart_seasonal = (
        alt.Chart(_df)
        .mark_rect(stroke="white", strokeWidth=0.5)
        .encode(
            x=alt.X("month_name:O", sort=list(_month_labels.values()), title="Month"),
            y=alt.Y("year:O", sort="descending", title=None),
            color=alt.Color("journeys:Q", scale=alt.Scale(scheme="lightmulti"), title="Journeys"),
            tooltip=["year", "month_name", "journeys"],
        )
        .properties(title="Journey count by year × month", width=600, height=380)
    )
    chart_seasonal
    return


@app.cell(hide_code=True)
def _(mo):
    mo.md("""
    ## 8 · Origin → Destination — top-15 station O-D heatmap
    """)
    return


@app.cell(hide_code=True)
def _(engine, mo):
    od_df = mo.sql(
        f"""
        WITH top_stations AS (
            SELECT start_station_name AS station
            FROM `serve.fct_journeys`
            WHERE start_station_name IS NOT NULL
            GROUP BY 1
            ORDER BY COUNT(*) DESC
            LIMIT 15
        )
        SELECT
            j.start_station_name AS origin,
            j.end_station_name   AS destination,
            COUNT(*)             AS journeys
        FROM `serve.fct_journeys` j
        INNER JOIN top_stations s1 ON j.start_station_name = s1.station
        INNER JOIN top_stations s2 ON j.end_station_name   = s2.station
        WHERE j.start_station_name != j.end_station_name
        GROUP BY 1, 2
        ORDER BY 3 DESC
        """,
        engine=engine
    )
    return (od_df,)


@app.cell
def _(alt, od_df, pl):
    _top_stations = (
        od_df.group_by("origin")
        .agg(pl.col("journeys").sum())
        .sort("journeys", descending=True)["origin"]
        .to_list()
    )
    chart_od = (
        alt.Chart(od_df)
        .mark_rect(stroke="white", strokeWidth=0.5)
        .encode(
            x=alt.X("destination:N", sort=_top_stations, title="Destination", axis=alt.Axis(labelAngle=-45)),
            y=alt.Y("origin:N", sort=_top_stations, title="Origin"),
            color=alt.Color(
                "journeys:Q",
                scale=alt.Scale(scheme="lightmulti"),
                title="Journeys",
            ),
            tooltip=["origin", "destination", "journeys"],
        )
        .properties(title="O-D matrix: journeys between top-15 start stations", width=560, height=520)
    )
    chart_od
    return


if __name__ == "__main__":
    app.run()
