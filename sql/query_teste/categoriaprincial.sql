WITH base AS (
    SELECT
        c.customer_unique_id,
        p.product_category_name,
        SUM(i.price + i.freight_value) AS gasto_categoria
    FROM silver.olist_orders_dataset o
    JOIN silver.olist_customers_dataset c
        ON o.customer_id = c.customer_id
    JOIN silver.olist_order_items_dataset i
        ON o.order_id = i.order_id
    JOIN silver.olist_products_dataset p
        ON i.product_id = p.product_id
    GROUP BY c.customer_unique_id, p.product_category_name
),
ranked AS (
    SELECT
        customer_unique_id,
        product_category_name,
        gasto_categoria,
        ROW_NUMBER() OVER (
            PARTITION BY customer_unique_id
            ORDER BY gasto_categoria DESC
        ) AS rn
    FROM base
)
SELECT *
FROM ranked
WHERE rn = 1;
