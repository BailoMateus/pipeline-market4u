CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.olist_geolocation_dataset;

CREATE TABLE silver.olist_geolocation_dataset AS
SELECT
    regexp_replace(geolocation_zip_code_prefix, '[^0-9]', '', 'g') AS geolocation_zip_code_prefix,
    CAST(geolocation_lat AS NUMERIC(18,6)) AS geolocation_lat,
    CAST(geolocation_lng AS NUMERIC(18,6)) AS geolocation_lng,
    lower(trim(geolocation_city)) AS geolocation_city,
    upper(trim(geolocation_state)) AS geolocation_state
FROM bronze.olist_geolocation_dataset;
