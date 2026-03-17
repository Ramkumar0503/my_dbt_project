SELECT 
    order_id,
    customer_name,
    amount
FROM {{ source('dbo', 'orders') }}