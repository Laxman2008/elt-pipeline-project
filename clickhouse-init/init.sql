CREATE DATABASE IF NOT EXISTS analytics;

CREATE TABLE IF NOT EXISTS analytics.processed_records
(
    id String,
    pickup_datetime DateTime,
    dropoff_datetime DateTime,
    passenger_count UInt8,
    trip_distance Float64,
    rate_code String,
    store_and_fwd String,
    payment_type String,
    fare_amount Float64,
    raw_insert_time DateTime DEFAULT now()
) ENGINE = MergeTree()
ORDER BY (pickup_datetime, id);

CREATE TABLE IF NOT EXISTS analytics.pipeline_metrics
(
    run_id String,
    run_time DateTime,
    stage String,
    rows_processed UInt64,
    notes String
) ENGINE = MergeTree()
ORDER BY run_time;
