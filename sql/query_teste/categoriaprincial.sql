SELECT
    customer_unique_id,
    product_category_name,
    SUM(price + freight_value) AS gasto_categoria,
    ROW_NUMBER() OVER (
        PARTITION BY customer_unique_id
        ORDER BY SUM(price + freight_value) DESC
    ) AS rn
FROM pedidos_clientes
GROUP BY customer_unique_id, product_category_name