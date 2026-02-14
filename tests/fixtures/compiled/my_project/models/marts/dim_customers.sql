SELECT
    c.customer_id,
    c.customer_name,
    c.email,
    c.created_at,
    COUNT(o.order_id) as total_orders
FROM analytics.staging.stg_customers c
LEFT JOIN analytics.staging.stg_orders o 
    ON c.customer_id = o.customer_id
GROUP BY 1, 2, 3, 4
