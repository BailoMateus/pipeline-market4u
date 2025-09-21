CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.olist_order_payments_dataset;

CREATE TABLE silver.olist_order_payments_dataset AS
SELECT
    order_id,
    payment_sequential::INTEGER,
    lower(trim(payment_type)) AS payment_type,
    payment_installments::INTEGER,
    CAST(payment_value AS NUMERIC(18,6)) AS payment_value
FROM bronze.olist_order_payments_dataset;
