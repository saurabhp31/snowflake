{{
    config(
        materialized='view'
    )
}}

SELECT 
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.CUSTOMER_SINCE,
    SUM(t.AMOUNT) AS TOTAL_REVENUE,
    DATEDIFF('day', c.CUSTOMER_SINCE, CURRENT_DATE()) AS DAYS_AS_CUSTOMER,
    SUM(t.AMOUNT) / DATEDIFF('day', c.CUSTOMER_SINCE, CURRENT_DATE()) AS DAILY_AVERAGE_SPENDING
FROM {{ source('globalbank', 'customers') }} c
JOIN {{ source('globalbank', 'transactions') }} t ON c.CUSTOMER_ID = t.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.CUSTOMER_SINCE
