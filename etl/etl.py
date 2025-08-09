#!/usr/bin/env python3
import os
import sys
import yaml
import hmac
import hashlib
import pandas as pd
from datetime import timedelta, datetime
from dateutil import parser
from clickhouse_driver import Client
import psycopg2
from psycopg2.extras import execute_values
import argparse
import uuid

# Load config
CFG_PATH = "/opt/etl/config.yml"
with open(CFG_PATH) as f:
    cfg = yaml.safe_load(f)

DEFAULTS = cfg.get("defaults", {})
ANON = cfg.get("anonymization", {})

HMAC_KEY = os.environ.get(ANON.get("hmac_key_env", "PII_HMAC_KEY"), "default-insecure-key").encode()

# DB env
PG_CONN = {
    "dbname": os.environ.get("PG_DB", "staging_db"),
    "user": os.environ.get("PG_USER", "staging"),
    "password": os.environ.get("PG_PASSWORD", "stagingpwd"),
    "host": os.environ.get("PG_HOST", "postgres"),
    "port": int(os.environ.get("PG_PORT", 5432))
}
CH_HOST = os.environ.get("CH_HOST", "clickhouse")
CH_PORT = int(os.environ.get("CH_PORT", 9000))
CH_DB = os.environ.get("CH_DB", "analytics")
INPUT_CSV = os.environ.get("INPUT_CSV", "/data/green_tripdata_sample.csv")

def hmac_hash(value: str):
    if pd.isna(value) or value is None:
        return None
    hm = hmac.new(HMAC_KEY, str(value).encode("utf-8"), hashlib.sha256)
    return hm.hexdigest()

def fill_defaults(df: pd.DataFrame, defaults: dict):
    for col, val in defaults.items():
        if col in df.columns:
            df[col] = df[col].fillna(val)
        else:
            df[col] = val
    return df

def transform_df(df: pd.DataFrame):
    df = fill_defaults(df, DEFAULTS)
    # create deterministic id if none exists
    if "id" not in df.columns:
        df["id"] = (df["pickup_datetime"].astype(str) + "|" + df["trip_distance"].astype(str)).apply(lambda x: hmac_hash(x))
    else:
        df["id"] = df["id"].apply(lambda x: hmac_hash(x) if pd.notna(x) else None)

    # parse datetimes
    for c in ["pickup_datetime", "dropoff_datetime"]:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").fillna(pd.to_datetime(DEFAULTS.get(c)))

    # coerce numeric
    if "trip_distance" in df.columns:
        df["trip_distance"] = pd.to_numeric(df["trip_distance"], errors="coerce").fillna(DEFAULTS.get("trip_distance"))

    if "passenger_count" in df.columns:
        df["passenger_count"] = pd.to_numeric(df["passenger_count"], errors="coerce").fillna(DEFAULTS.get("passenger_count")).astype(int)

    if "fare_amount" in df.columns:
        df["fare_amount"] = pd.to_numeric(df["fare_amount"], errors="coerce").fillna(DEFAULTS.get("fare_amount"))

    # other masks/anon operations can be added here (emails, phones)
    return df

def ingest_csv_to_postgres(csv_path):
    if not os.path.exists(csv_path):
        print(f"CSV not found at {csv_path}", file=sys.stderr)
        return 0
    df = pd.read_csv(csv_path)
    records = df.where(pd.notnull(df), None).to_dict(orient='records')
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    cols = list(df.columns)
    insert_sql = f"INSERT INTO raw_input ({', '.join(cols)}) VALUES %s"
    values = [[r.get(c) for c in cols] for r in records]
    try:
        if values:
            execute_values(cur, insert_sql, values)
            conn.commit()
            print(f"Inserted {len(values)} rows into raw_input")
    finally:
        cur.close()
        conn.close()
    return len(values)

def read_from_postgres():
    conn = psycopg2.connect(**PG_CONN)
    try:
        df = pd.read_sql("SELECT * FROM raw_input", conn)
    finally:
        conn.close()
    return df

def write_to_clickhouse(df: pd.DataFrame, table="processed_records"):
    client = Client(host=CH_HOST, port=CH_PORT, database=CH_DB)
    if df.empty:
        return 0
    df = df.where(pd.notnull(df), None)
    client.insert_dataframe(f"INSERT INTO {CH_DB}.{table} VALUES", df)
    return len(df)

def write_metric(run_id, stage, rows, notes=""):
    client = Client(host=CH_HOST, port=CH_PORT, database=CH_DB)
    client.execute(f"INSERT INTO {CH_DB}.pipeline_metrics (run_id, run_time, stage, rows_processed, notes) VALUES", [(run_id, datetime.utcnow(), stage, rows, notes)])

def clear_staging_table():
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    try:
        cur.execute("TRUNCATE TABLE raw_input;")
        conn.commit()
    finally:
        cur.close()
        conn.close()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--mode", choices=["ingest","transform","full","clear"], default="full")
    args = parser.parse_args()

    run_id = str(uuid.uuid4())

    if args.mode in ("ingest","full"):
        n = ingest_csv_to_postgres(INPUT_CSV)
        write_metric(run_id, "ingest", n, "CSV -> Postgres")
    if args.mode in ("transform","full"):
        df = read_from_postgres()
        if df.empty:
            print("No data in staging; skipping transform.")
            write_metric(run_id, "transform", 0, "no rows")
            return
        df_t = transform_df(df)
        # ensure clickhouse target columns
        target_cols = ["id","pickup_datetime","dropoff_datetime","passenger_count","trip_distance","rate_code","store_and_fwd","payment_type","fare_amount"]
        for c in target_cols:
            if c not in df_t.columns:
                df_t[c] = None
        rows = write_to_clickhouse(df_t[target_cols])
        write_metric(run_id, "transform", rows, "Postgres -> ClickHouse")
        print(f"Wrote {rows} rows to ClickHouse.")
    if args.mode == "clear":
        clear_staging_table()
        print("Staging table cleared.")
    print("Done.")

if __name__ == "__main__":
    main()
