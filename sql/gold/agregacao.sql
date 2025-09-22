SELECT
    customer_unique_id,
    estado_cliente,
    cidade_cliente,
    COUNT(DISTINCT order_id) AS total_pedidos,
    SUM(price + freight_value) AS total_gasto,
    MIN(order_purchase_timestamp) AS data_primeira_compra,
    MAX(order_purchase_timestamp) AS data_ultima_compra,
    DATE_PART('day', MAX(order_purchase_timestamp) - (SELECT MAX(order_purchase_timestamp) FROM pedidos_clientes)) * -1 AS dias_desde_ultima_compra,
    AVG(
        CASE WHEN order_delivered_customer_date IS NOT NULL
            THEN DATE_PART('day', order_delivered_customer_date - order_purchase_timestamp)
        END
        ) AS avg_delivery_time_days
FROM pedidos_clientes
GROUP BY customer_unique_id, estado_cliente, cidade_cliente