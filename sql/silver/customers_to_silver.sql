CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.olist_customers_dataset;

CREATE TABLE silver.olist_customers_dataset AS
SELECT
    customer_id,
    customer_unique_id,
    regexp_replace(customer_zip_code_prefix::text, '[^0-9]', '', 'g') AS customer_zip_code_prefix,
    lower(trim(customer_city)) AS customer_city,
    upper(trim(customer_state)) AS customer_state
FROM bronze.olist_customers_dataset;
