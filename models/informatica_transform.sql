WITH customer_orders AS (
    SELECT
        customer_id,
        COUNT(*) AS total_orders,
        SUM(order_amount) AS total_amount
    FROM DBT_DB.DBT_SCHEMA.ORDERS
    GROUP BY customer_id
)
SELECT * FROM customer_orders
