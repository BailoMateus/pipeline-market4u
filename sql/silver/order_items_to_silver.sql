CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.olist_order_items_dataset;

CREATE TABLE silver.olist_order_items_dataset AS
SELECT DISTINCT
    order_id,
    order_item_id::INTEGER,
    product_id,
    seller_id,
    CAST(shipping_limit_date AS TIMESTAMP) AS shipping_limit_date,
    CAST(price AS NUMERIC(18,6)) AS price,
    CAST(freight_value AS NUMERIC(18,6)) AS freight_value
FROM bronze.olist_order_items_dataset;
