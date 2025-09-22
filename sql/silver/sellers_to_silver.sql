CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.olist_sellers_dataset;

CREATE TABLE silver.olist_sellers_dataset AS
SELECT
    seller_id,
    regexp_replace(seller_zip_code_prefix::text, '[^0-9]', '', 'g') AS seller_zip_code_prefix,
    lower(trim(seller_city)) AS seller_city,
    upper(trim(seller_state)) AS seller_state
FROM bronze.olist_sellers_dataset;
