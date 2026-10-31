-- Run once, after "terraform apply" (warehouse, database, schemas and roles are managed by Terraform)
USE ROLE FRESHROUTE_ENGINEER;
USE WAREHOUSE FRESHROUTE_WH;
USE SCHEMA FRESHROUTE.BRONZE;

-- Bronze rule: every column as TEXT, exactly as received, + lineage columns. Nothing is lost, silver repairs.
CREATE FILE FORMAT IF NOT EXISTS CSV_RAW
  TYPE = CSV PARSE_HEADER = TRUE FIELD_OPTIONALLY_ENCLOSED_BY = '"'
  ENCODING = 'UTF8' ERROR_ON_COLUMN_COUNT_MISMATCH = FALSE;

CREATE STAGE IF NOT EXISTS TMS_STAGE
  DIRECTORY = (ENABLE = TRUE)
  COMMENT = 'Landing zone: nightly exports of the transport management system';

-- Source 1: batch files
CREATE TABLE IF NOT EXISTS DEPOTS   (depot_id VARCHAR, city VARCHAR, docks VARCHAR,
                                     _source_file VARCHAR, _loaded_at TIMESTAMP_TZ);
CREATE TABLE IF NOT EXISTS VEHICLES (vehicle_id VARCHAR, plate VARCHAR, vehicle_type VARCHAR, depot_id VARCHAR,
                                     capacity_kg VARCHAR, _source_file VARCHAR, _loaded_at TIMESTAMP_TZ);
CREATE TABLE IF NOT EXISTS ROUTES   (route_id VARCHAR, vehicle_id VARCHAR, depot_id VARCHAR, planned_start VARCHAR,
                                     planned_stops VARCHAR, status VARCHAR, _source_file VARCHAR, _loaded_at TIMESTAMP_TZ);

-- Source 2: incremental pull from Postgres
CREATE TABLE IF NOT EXISTS DELIVERIES (
  delivery_id VARCHAR, route_id VARCHAR, customer_ref VARCHAR, promised_by VARCHAR, delivered_at VARCHAR,
  parcel_kg VARCHAR, delivery_fee_gbp VARCHAR, outcome VARCHAR, source_system VARCHAR,
  _batch NUMBER, _pg_loaded_at TIMESTAMP_TZ, _run_id VARCHAR,
  _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());

-- Source 3: GPS micro-batch stream
CREATE TABLE IF NOT EXISTS GPS_PINGS (
  ping_id VARCHAR, vehicle_id VARCHAR, ping_ts VARCHAR, payload VARCHAR, source_system VARCHAR,
  _batch NUMBER, _run_id VARCHAR, _loaded_at TIMESTAMP_TZ DEFAULT CURRENT_TIMESTAMP());

-- Sources 4-6: raw API answers (schema on read in bronze, typed in silver)
CREATE TABLE IF NOT EXISTS WEATHER_RAW       (source_key VARCHAR, payload VARIANT, _run_id VARCHAR, _loaded_at TIMESTAMP_TZ);
CREATE TABLE IF NOT EXISTS FX_RAW            (source_key VARCHAR, payload VARIANT, _run_id VARCHAR, _loaded_at TIMESTAMP_TZ);
CREATE TABLE IF NOT EXISTS BANK_HOLIDAYS_RAW (source_key VARCHAR, payload VARIANT, _run_id VARCHAR, _loaded_at TIMESTAMP_TZ);
