SELECT
    customer_id,
    SUM(amount) AS total_amount
FROM {{ ref('stg_orders') }}
GROUP BY customer_id