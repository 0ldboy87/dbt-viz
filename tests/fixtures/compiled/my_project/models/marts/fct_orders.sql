SELECT
    o.order_id,
    o.customer_id,
    c.customer_name,
    c.email as customer_email,
    o.order_date,
    o.status,
    DATEDIFF(day, o.order_date, CURRENT_DATE) as days_since_order
FROM analytics.staging.stg_orders o
LEFT JOIN analytics.staging.stg_customers c 
    ON o.customer_id = c.customer_id
