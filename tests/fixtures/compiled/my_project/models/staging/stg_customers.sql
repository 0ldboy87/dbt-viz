SELECT
    id as customer_id,
    name as customer_name,
    email,
    created_at
FROM production.raw.customers
