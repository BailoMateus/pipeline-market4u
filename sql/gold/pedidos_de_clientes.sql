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