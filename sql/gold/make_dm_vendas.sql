CREATE SCHEMA IF NOT EXISTS gold;

DROP TABLE IF EXISTS gold.dm_vendas_clientes;

CREATE TABLE gold.dm_vendas_clientes AS
WITH pedidos_clientes AS (
    SELECT
        c.customer_unique_id,
        c.customer_state AS estado_cliente,
        c.customer_city AS cidade_cliente,
        o.order_id,
        o.order_purchase_timestamp,
        o.order_delivered_customer_date,
        o.order_estimated_delivery_date,
        i.price,
        i.freight_value,
        p.product_category_name
    FROM silver.olist_customers_dataset c
    JOIN silver.olist_orders_dataset o
        ON c.customer_id = o.customer_id
    JOIN silver.olist_order_items_dataset i
        ON o.order_id = i.order_id
    JOIN silver.olist_products_dataset p
        ON i.product_id = p.product_id
)
, agregado_cliente AS (
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
)
, categoria_top AS (
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
)

SELECT
    a.customer_unique_id,
    a.estado_cliente,
    a.cidade_cliente,
    a.total_pedidos,
    a.total_gasto,
    a.data_primeira_compra,
    a.data_ultima_compra,
    a.dias_desde_ultima_compra,
    a.avg_delivery_time_days,
    ct.product_category_name AS categoria_mais_comprada
FROM agregado_cliente AS a
LEFT JOIN categoria_top ct
    ON a.customer_unique_id = ct.customer_unique_id
   AND ct.rn = 1;

--Successfully run. Total query runtime: 4 secs 536 msec. 95539 rows affected
-- Optei por não usar Window's functions aqui por que aumentava muito o tempo para rodar