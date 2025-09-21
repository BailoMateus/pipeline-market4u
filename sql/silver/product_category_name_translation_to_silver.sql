CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.product_category_name_translation;

CREATE TABLE silver.product_category_name_translation AS
SELECT
    lower(trim(product_category_name)) AS product_category_name,
    lower(trim(product_category_name_english)) AS product_category_name_english
FROM bronze.product_category_name_translation;
