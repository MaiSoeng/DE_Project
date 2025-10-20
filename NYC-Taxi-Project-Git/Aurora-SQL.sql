USE taxidata;

CREATE INDEX idx_tt_pickup_dt ON taxi_trips (tpep_pickup_datetime);
CREATE INDEX idx_tt_vendor_dt ON taxi_trips (VendorID, tpep_pickup_datetime);

-- Trip counts by date
CREATE OR REPLACE VIEW v_trip_counts_daily AS
SELECT
  DATE(tpep_pickup_datetime) AS pickup_date,
  COUNT(*) AS trip_count
FROM taxi_trips
GROUP BY DATE(tpep_pickup_datetime);

-- Trip counts by month
CREATE OR REPLACE VIEW v_trip_counts_monthly AS
SELECT
    DATE_FORMAT(tpep_pickup_datetime, '%Y-%m') AS pickup_month,
    COUNT(*) AS trip_count
    FROM taxi_trips
GROUP BY DATE_FORMAT(tpep_pickup_datetime, '%Y-%m')
ORDER BY pickup_month;

-- Trip counts by vendor and date
CREATE OR REPLACE VIEW v_trip_counts_vendor_daily AS
SELECT
    DATE(tpep_pickup_datetime) AS pickup_date,
    VendorID,
    COUNT(*) AS trip_count
FROM taxi_trips
GROUP BY DATE(tpep_pickup_datetime), VendorID
ORDER BY pickup_date, VendorID;

-- Trip counts by vendor and month
CREATE OR REPLACE VIEW v_trip_counts_vendor_monthly AS
SELECT
    DATE_FORMAT(tpep_pickup_datetime, '%Y-%m') AS pickup_month,
    VendorID,
    COUNT(*) AS trip_count
FROM taxi_trips
GROUP BY DATE_FORMAT(tpep_pickup_datetime, '%Y-%m'), VendorID
ORDER BY pickup_month, VendorID;

-- Daily Summary Metrics 
CREATE OR REPLACE VIEW v_daily_summary_metrics AS
SELECT
    pickup_year,
    pickup_month,
    pickup_day,
    SUM(trip_count) AS total_trip_count,
    SUM(base_revenue) AS total_base_revenue,
    SUM(gross_revenue) AS total_gross_revenue,
    -- Weighted averages (by trip count) to get accurate daily averages
    SUM(trip_count * avg_trip_duration) / SUM(trip_count) AS avg_trip_duration_min,
    SUM(trip_count * avg_base_fare_per_mile) / SUM(trip_count) AS avg_base_fare_per_mile,
    SUM(trip_count * avg_fare_per_mile) / SUM(trip_count) AS avg_fare_per_mile,
    SUM(trip_count * avg_fare_amount) / SUM(trip_count) AS avg_fare_amount,
    SUM(trip_count * avg_trip_distance) / SUM(trip_count) AS avg_trip_distance
FROM daily_trip_stats
GROUP BY pickup_year, pickup_month, pickup_day
ORDER BY pickup_year, pickup_month, pickup_day;

-- Monthly Summary Metrics 
CREATE OR REPLACE VIEW v_monthly_summary_metrics AS
SELECT
    pickup_year,
    pickup_month,
    SUM(trip_count) AS total_trip_count,
    SUM(base_revenue) AS total_base_revenue,
    SUM(gross_revenue) AS total_gross_revenue,
    -- Weighted averages (by trip count) to get accurate monthly averages
    SUM(trip_count * avg_trip_duration) / SUM(trip_count) AS avg_trip_duration_min,
    SUM(trip_count * avg_base_fare_per_mile) / SUM(trip_count) AS avg_base_fare_per_mile,
    SUM(trip_count * avg_fare_per_mile) / SUM(trip_count) AS avg_fare_per_mile,
    SUM(trip_count * avg_fare_amount) / SUM(trip_count) AS avg_fare_amount,
    SUM(trip_count * avg_trip_distance) / SUM(trip_count) AS avg_trip_distance,
    -- Monthly performance ratios
    (SUM(gross_revenue) / SUM(base_revenue)) AS revenue_multiplier
FROM daily_trip_stats
GROUP BY pickup_year, pickup_month
ORDER BY pickup_year, pickup_month;

-- Hourly Summary Metrics 
CREATE OR REPLACE VIEW v_hourly_summary_metrics AS
SELECT
    pickup_year,
    pickup_month,
    pickup_day,
    pickup_hour,
    trip_count,
    avg_trip_duration,
    avg_base_fare_per_mile,
    avg_fare_per_mile
FROM hourly_trip_stats
ORDER BY pickup_year, pickup_month, pickup_day, pickup_hour;

-- Vendor Performance Metrics
CREATE OR REPLACE VIEW v_vendor_performance_metrics AS
SELECT
    VendorID,
    trip_count,
    avg_trip_duration,
    avg_base_fare_per_mile,
    avg_fare_per_mile,
    avg_fare_amount,
    base_fare_per_mile_stddev,
    fare_per_mile_stddev,
    -- Additional calculated metrics
    (base_fare_per_mile_stddev / avg_base_fare_per_mile) AS base_fare_cv,
    (fare_per_mile_stddev / avg_fare_per_mile) AS fare_per_mile_cv,
    -- Performance ranking
    ROW_NUMBER() OVER (ORDER BY trip_count DESC) AS vendor_rank_by_volume,
    ROW_NUMBER() OVER (ORDER BY avg_fare_amount DESC) AS vendor_rank_by_fare
FROM vendor_performance
ORDER BY trip_count DESC;

-- Vendor Daily Performance
CREATE OR REPLACE VIEW v_vendor_daily_performance AS
SELECT
    pickup_year,
    pickup_month,
    pickup_day,
    VendorID,
    trip_count,
    avg_trip_duration,
    avg_base_fare_per_mile,
    avg_fare_per_mile,
    avg_fare_amount,
    avg_trip_distance,
    base_revenue,
    gross_revenue,
    (gross_revenue / base_revenue) AS revenue_multiplier
FROM daily_trip_stats
ORDER BY pickup_year, pickup_month, pickup_day, VendorID;

-- Cross-Vendor Comparison
CREATE OR REPLACE VIEW v_vendor_comparison AS
WITH vendor_summary AS (
    SELECT
        VendorID,
        SUM(trip_count) AS total_trips,
        SUM(base_revenue) AS total_base_revenue,
        SUM(gross_revenue) AS total_gross_revenue,
        -- Weighted averages by trip count (correct way to aggregate pre-computed averages)
        SUM(trip_count * avg_trip_duration) / SUM(trip_count) AS avg_duration,
        SUM(trip_count * avg_fare_amount) / SUM(trip_count) AS avg_fare
    FROM daily_trip_stats
    GROUP BY VendorID
)
SELECT
    VendorID,
    total_trips,
    avg_duration,
    avg_fare,
    total_base_revenue,
    total_gross_revenue,
    (total_gross_revenue / total_base_revenue) AS revenue_multiplier,
    -- Market share
    (total_trips / SUM(total_trips) OVER ()) * 100 AS market_share_percent,
    -- Performance vs average
    (avg_duration / AVG(avg_duration) OVER ()) AS duration_vs_avg,
    (avg_fare / AVG(avg_fare) OVER ()) AS fare_vs_avg
FROM vendor_summary
ORDER BY total_trips DESC;

-- Daily Trip Volume Anomaly Detection 
CREATE OR REPLACE VIEW v_daily_volume_anomalies AS
WITH daily_aggregated AS (
    SELECT
        pickup_year,
        pickup_month,
        pickup_day,
        SUM(trip_count) AS total_trip_count
    FROM daily_trip_stats
    GROUP BY pickup_year, pickup_month, pickup_day
),
volume_analysis AS (
    SELECT
        pickup_year,
        pickup_month,
        pickup_day,
        total_trip_count,
        AVG(total_trip_count) OVER (ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_7d,
        STDDEV(total_trip_count) OVER (ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_stddev_7d
    FROM daily_aggregated
)
SELECT
    pickup_year,
    pickup_month,
    pickup_day,
    total_trip_count,
    rolling_avg_7d,
    rolling_stddev_7d,
    CASE 
        WHEN total_trip_count > (rolling_avg_7d + 2 * rolling_stddev_7d) THEN 'HIGH_VOLUME_ANOMALY'
        WHEN total_trip_count < (rolling_avg_7d - 2 * rolling_stddev_7d) THEN 'LOW_VOLUME_ANOMALY'
        ELSE 'NORMAL'
    END AS anomaly_type,
    ROUND(((total_trip_count - rolling_avg_7d) / rolling_avg_7d) * 100, 2) AS volume_deviation_percent
FROM volume_analysis
WHERE rolling_stddev_7d IS NOT NULL
ORDER BY pickup_year, pickup_month, pickup_day;

-- Vendor-specific Volume Anomaly Detection 
CREATE OR REPLACE VIEW v_vendor_volume_anomalies AS
WITH vendor_analysis AS (
    SELECT
        VendorID,
        pickup_year,
        pickup_month,
        pickup_day,
        trip_count,
        AVG(trip_count) OVER (PARTITION BY VendorID ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS vendor_rolling_avg_7d,
        STDDEV(trip_count) OVER (PARTITION BY VendorID ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS vendor_rolling_stddev_7d
    FROM daily_trip_stats
)
SELECT
    VendorID,
    pickup_year,
    pickup_month,
    pickup_day,
    trip_count,
    vendor_rolling_avg_7d,
    vendor_rolling_stddev_7d,
    CASE 
        WHEN trip_count > (vendor_rolling_avg_7d + 2 * vendor_rolling_stddev_7d) THEN 'HIGH_VOLUME_ANOMALY'
        WHEN trip_count < (vendor_rolling_avg_7d - 2 * vendor_rolling_stddev_7d) THEN 'LOW_VOLUME_ANOMALY'
        ELSE 'NORMAL'
    END AS anomaly_type,
    ROUND(((trip_count - vendor_rolling_avg_7d) / vendor_rolling_avg_7d) * 100, 2) AS volume_deviation_percent
FROM vendor_analysis
WHERE vendor_rolling_stddev_7d IS NOT NULL
ORDER BY VendorID, pickup_year, pickup_month, pickup_day;

-- Performance Anomaly Detection 
CREATE OR REPLACE VIEW v_performance_anomalies AS
WITH vendor_performance_analysis AS (
    SELECT
        VendorID,
        pickup_year,
        pickup_month,
        pickup_day,
        avg_trip_duration,
        avg_fare_amount,
        avg_base_fare_per_mile,
        avg_fare_per_mile,
        trip_count,
        -- Calculate weighted rolling averages (using trip_count as weight)
        SUM(trip_count * avg_trip_duration) OVER (PARTITION BY VendorID ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / 
        SUM(trip_count) OVER (PARTITION BY VendorID ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_duration,
        SUM(trip_count * avg_fare_amount) OVER (PARTITION BY VendorID ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) / 
        SUM(trip_count) OVER (PARTITION BY VendorID ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_avg_fare,
        -- Calculate standard deviations
        STDDEV(avg_trip_duration) OVER (PARTITION BY VendorID ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_stddev_duration,
        STDDEV(avg_fare_amount) OVER (PARTITION BY VendorID ORDER BY pickup_year, pickup_month, pickup_day ROWS BETWEEN 6 PRECEDING AND CURRENT ROW) AS rolling_stddev_fare
    FROM daily_trip_stats
)
SELECT
    VendorID,
    pickup_year,
    pickup_month,
    pickup_day,
    avg_trip_duration,
    avg_fare_amount,
    avg_base_fare_per_mile,
    avg_fare_per_mile,
    rolling_avg_duration,
    rolling_stddev_duration,
    rolling_avg_fare,
    rolling_stddev_fare,
    -- Detect anomalies
    CASE 
        WHEN avg_trip_duration > (rolling_avg_duration + 2 * rolling_stddev_duration) THEN 'HIGH_DURATION_ANOMALY'
        WHEN avg_trip_duration < (rolling_avg_duration - 2 * rolling_stddev_duration) THEN 'LOW_DURATION_ANOMALY'
        ELSE 'NORMAL_DURATION'
    END AS duration_anomaly_type,
    CASE 
        WHEN avg_fare_amount > (rolling_avg_fare + 2 * rolling_stddev_fare) THEN 'HIGH_FARE_ANOMALY'
        WHEN avg_fare_amount < (rolling_avg_fare - 2 * rolling_stddev_fare) THEN 'LOW_FARE_ANOMALY'
        ELSE 'NORMAL_FARE'
    END AS fare_anomaly_type,
    ROUND(((avg_trip_duration - rolling_avg_duration) / rolling_avg_duration) * 100, 2) AS duration_deviation_percent,
    ROUND(((avg_fare_amount - rolling_avg_fare) / rolling_avg_fare) * 100, 2) AS fare_deviation_percent
FROM vendor_performance_analysis
WHERE rolling_stddev_duration IS NOT NULL AND rolling_stddev_fare IS NOT NULL
ORDER BY VendorID, pickup_year, pickup_month, pickup_day; 

-- Time Based Fare Efficiency Analysis
CREATE OR REPLACE VIEW v_time_based_fare_efficiency AS
WITH time_stats AS (
    SELECT
        pickup_hour,
        time_of_day,
        COUNT(*) AS total_trips,
        AVG(fare_per_mile) AS avg_fare_per_mile,
        AVG(base_fare_per_mile) AS avg_base_fare_per_mile,
        AVG(trip_duration_min) AS avg_duration,
        AVG(trip_distance) AS avg_distance
    FROM taxi_trips
    WHERE pickup_hour IS NOT NULL
      AND time_of_day IS NOT NULL
    GROUP BY pickup_hour, time_of_day
),
time_benchmarks AS (
    SELECT
        SUM(total_trips * avg_fare_per_mile) / SUM(total_trips) AS benchmark_avg_fare_per_mile,
        STDDEV(avg_fare_per_mile) AS stddev_fare_per_mile,
        SUM(total_trips * avg_duration) / SUM(total_trips) AS benchmark_avg_duration,
        SUM(total_trips * avg_distance) / SUM(total_trips) AS benchmark_avg_distance
    FROM time_stats
)
SELECT
    t.pickup_hour,
    t.time_of_day,
    t.total_trips,
    t.avg_fare_per_mile,
    t.avg_duration,
    t.avg_distance,
    tb.benchmark_avg_fare_per_mile,
    tb.stddev_fare_per_mile,
    -- Efficiency metrics
    (t.avg_fare_per_mile - tb.benchmark_avg_fare_per_mile) / tb.stddev_fare_per_mile AS fare_efficiency_zscore,
    (t.avg_duration / t.avg_distance) AS efficiency_score,
    -- Pricing opportunity metrics
    CASE
        WHEN (t.avg_fare_per_mile - tb.benchmark_avg_fare_per_mile) / tb.stddev_fare_per_mile > 2 THEN 'HIGH_FARE_OPPORTUNITY'
        WHEN (t.avg_fare_per_mile - tb.benchmark_avg_fare_per_mile) / tb.stddev_fare_per_mile < -2 THEN 'LOW_FARE_OPPORTUNITY'
        ELSE 'NORMAL'
    END AS pricing_opportunity_type,
    ROUND((tb.benchmark_avg_fare_per_mile - t.avg_fare_per_mile) / tb.benchmark_avg_fare_per_mile * 100, 2) AS fare_deviation_percent
FROM time_stats t
CROSS JOIN time_benchmarks tb
ORDER BY t.pickup_hour;

-- Route Based Fare Efficiency Analysis 
CREATE OR REPLACE VIEW v_route_fare_efficiency AS
WITH route_stats AS (
    SELECT
        pickup_borough,
        dropoff_borough,
        CONCAT(pickup_borough, ' to ', dropoff_borough) AS route,
        COUNT(*) AS total_trips,
        AVG(fare_per_mile) AS avg_fare_per_mile,
        AVG(base_fare_per_mile) AS avg_base_fare_per_mile,
        AVG(trip_duration_min) AS avg_duration,
        AVG(trip_distance) AS avg_distance
    FROM taxi_trips
    WHERE pickup_borough IS NOT NULL 
      AND dropoff_borough IS NOT NULL
    GROUP BY pickup_borough, dropoff_borough
),
route_benchmarks AS (
    SELECT
        SUM(total_trips * avg_fare_per_mile) / SUM(total_trips) AS benchmark_avg_fare_per_mile,
        STDDEV(avg_fare_per_mile) AS stddev_fare_per_mile,
        SUM(total_trips * avg_duration) / SUM(total_trips) AS benchmark_avg_duration,
        SUM(total_trips * avg_distance) / SUM(total_trips) AS benchmark_avg_distance
    FROM route_stats
)
SELECT
    rs.route,
    rs.pickup_borough,
    rs.dropoff_borough,
    rs.total_trips,
    rs.avg_fare_per_mile,
    rs.avg_base_fare_per_mile,
    rs.avg_duration,
    rs.avg_distance,
    rb.benchmark_avg_fare_per_mile,
    rb.stddev_fare_per_mile,
    -- Route Efficiency Metrics
    (rs.avg_fare_per_mile - rb.benchmark_avg_fare_per_mile) / rb.stddev_fare_per_mile AS route_efficiency_zscore,
    (rs.avg_distance / rs.avg_duration) AS speed_efficiency_score,
    -- Pricing Opportunity Metrics
    CASE
        WHEN (rs.avg_fare_per_mile - rb.benchmark_avg_fare_per_mile) / rb.stddev_fare_per_mile > 2 THEN 'HIGH_FARE_OPPORTUNITY'
        WHEN (rs.avg_fare_per_mile - rb.benchmark_avg_fare_per_mile) / rb.stddev_fare_per_mile < -2 THEN 'LOW_FARE_OPPORTUNITY'
        ELSE 'NORMAL'
    END AS pricing_opportunity_type,
    ROUND((rb.benchmark_avg_fare_per_mile - rs.avg_fare_per_mile) / rb.benchmark_avg_fare_per_mile * 100, 2) AS fare_deviation_percent
FROM route_stats rs
CROSS JOIN route_benchmarks rb
ORDER BY rs.route;

