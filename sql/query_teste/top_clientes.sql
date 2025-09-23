WITH base AS (
    SELECT
        c.customer_unique_id,
        c.customer_state,
        SUM(i.price + i.freight_value) AS total_gasto
    FROM silver.olist_orders_dataset o
    JOIN silver.olist_customers_dataset c
        ON o.customer_id = c.customer_id
    JOIN silver.olist_order_items_dataset i
        ON o.order_id = i.order_id
    WHERE c.customer_state = 'SP'
    GROUP BY c.customer_unique_id, c.customer_state
),
categoria AS (
    SELECT
        c.customer_unique_id,
        p.product_category_name,
        SUM(i.price + i.freight_value) AS gasto_categoria,
        ROW_NUMBER() OVER (
            PARTITION BY c.customer_unique_id
            ORDER BY SUM(i.price + i.freight_value) DESC
        ) AS rn
    FROM silver.olist_orders_dataset o
    JOIN silver.olist_customers_dataset c
        ON o.customer_id = c.customer_id
    JOIN silver.olist_order_items_dataset i
        ON o.order_id = i.order_id
    JOIN silver.olist_products_dataset p
        ON i.product_id = p.product_id
    GROUP BY c.customer_unique_id, p.product_category_name
)
SELECT
    b.customer_unique_id,
    b.total_gasto,
    c.product_category_name AS categoria_preferida,
	b.customer_state
FROM base b
LEFT JOIN categoria c
    ON b.customer_unique_id = c.customer_unique_id
   AND c.rn = 1
ORDER BY b.total_gasto DESC
LIMIT 10;