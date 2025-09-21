CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.olist_products_dataset;

CREATE TABLE silver.olist_products_dataset AS
SELECT
    product_id,
    lower(trim(product_category_name)) AS product_category_name,
    product_name_lenght::INTEGER,
    product_description_lenght::INTEGER,
    product_photos_qty::INTEGER,
    CAST(product_weight_g AS NUMERIC(18,6)) AS product_weight_g,
    CAST(product_length_cm AS NUMERIC(18,6)) AS product_length_cm,
    CAST(product_height_cm AS NUMERIC(18,6)) AS product_height_cm,
    CAST(product_width_cm AS NUMERIC(18,6)) AS product_width_cm
FROM bronze.olist_products_dataset;
