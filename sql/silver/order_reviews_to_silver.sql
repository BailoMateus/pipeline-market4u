CREATE SCHEMA IF NOT EXISTS silver;

DROP TABLE IF EXISTS silver.olist_order_reviews_dataset;

CREATE TABLE silver.olist_order_reviews_dataset AS
SELECT
    review_id,
    order_id,
    review_score::INTEGER,
    review_comment_title,
    review_comment_message,
    CAST(review_creation_date AS TIMESTAMP) AS review_creation_date,
    CAST(review_answer_timestamp AS TIMESTAMP) AS review_answer_timestamp
FROM bronze.olist_order_reviews_dataset;
