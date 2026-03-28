# Looker Studio Dashboard Setup

## Connect to BigQuery
1. Open https://lookerstudio.google.com
2. Click **Create** → **Report**
3. Add data source → **BigQuery**
4. Select your GCP project → `santander_cycles_mart`

---

## Tile 1 — Rides Over Time (Line Chart)

**Data source:** `mart_monthly_summary`

| Setting | Value |
|---|---|
| Chart type | Time series / Line chart |
| Dimension | `ride_month` |
| Metric | `total_rides` |
| Secondary metric | `avg_duration_minutes` |
| Date range | All time (or last 3 years) |
| Sort | `ride_month` ascending |

**Style suggestions:**
- Dual Y-axis: left = total_rides, right = avg_duration_minutes
- Add reference line at seasonal averages
- Enable data labels on monthly peaks

**Insight this answers:** How has cycling demand changed over the years? Are there seasonal patterns?

---

## Tile 2 — Top Stations Bar Chart + Optional Map

**Data source:** `mart_station_stats`

### Bar chart (Top 20 stations by activity)

| Setting | Value |
|---|---|
| Chart type | Horizontal bar chart |
| Dimension | `station_name` |
| Metric | `total_activity` |
| Sort | `total_activity` descending |
| Rows shown | 20 |
| Breakdown dimension | `borough` (for colour coding) |

**Style suggestions:**
- Colour bars by borough using a discrete colour palette
- Add tooltip showing `total_departures`, `total_arrivals`, `net_flow`

### Filter controls (add to dashboard)
- **Date range control** — linked to Tile 1
- **Borough dropdown** — filters both tiles simultaneously

---

## BigQuery SQL to verify data before connecting

```sql
-- Quick sanity check on monthly summary
SELECT
  ride_month,
  total_rides,
  avg_duration_minutes,
  round_trip_pct
FROM `<project>.santander_cycles_mart.mart_monthly_summary`
ORDER BY ride_month DESC
LIMIT 24;

-- Top 10 stations
SELECT
  station_name,
  borough,
  total_departures,
  total_arrivals,
  net_flow
FROM `<project>.santander_cycles_mart.mart_station_stats`
ORDER BY total_activity DESC
LIMIT 10;
```
