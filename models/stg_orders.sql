SELECT 
    order_id,
    customer_id,
    amount,
    order_date,
    status
FROM {{ source('dbo', 'orders') }}