CREATE TABLE IF NOT EXISTS raw_input (
  id TEXT,
  pickup_datetime TEXT,
  dropoff_datetime TEXT,
  passenger_count INTEGER,
  trip_distance FLOAT,
  rate_code TEXT,
  store_and_fwd TEXT,
  payment_type TEXT,
  fare_amount FLOAT
);
