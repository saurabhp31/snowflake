{{
    config(
        materialized='view'
    )
}}

SELECT 
    YEAR(t.TRANSACTION_DATE) AS YEAR,
    MONTH(t.TRANSACTION_DATE) AS MONTH,
    COUNT(t.TRANSACTION_ID) AS TOTAL_TRANSACTIONS,
    SUM(t.AMOUNT) AS TOTAL_AMOUNT,
    AVG(t.AMOUNT) AS AVERAGE_TRANSACTION_AMOUNT
FROM {{ source('globalbank', 'transactions') }} t
GROUP BY YEAR(t.TRANSACTION_DATE), MONTH(t.TRANSACTION_DATE)
ORDER BY YEAR, MONTH
