SELECT
    id as order_id,
    customer_id,
    created_at::date as order_date,
    status
FROM production.raw.orders
