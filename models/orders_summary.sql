SELECT
    customer_name,
    SUM(amount) AS total_amount
FROM {{ ref('stg_orders') }}
GROUP BY customer_name