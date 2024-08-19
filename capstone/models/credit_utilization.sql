{{
    config(
        materialized='view'
    )
}}

SELECT 
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    cd.TOTAL_CREDIT_LIMIT,
    cd.TOTAL_CREDIT_USED,
    ROUND(cd.TOTAL_CREDIT_USED / cd.TOTAL_CREDIT_LIMIT * 100, 2) AS CREDIT_UTILIZATION_PERCENTAGE,
    CASE 
        WHEN ROUND(cd.TOTAL_CREDIT_USED / cd.TOTAL_CREDIT_LIMIT * 100, 2) > 80 THEN 'High Utilization'
        ELSE 'Normal Utilization'
    END AS UTILIZATION_RISK
FROM {{ source('globalbank', 'customers') }} c
JOIN {{ source('globalbank', 'credit_data') }} cd ON c.CUSTOMER_ID = cd.CUSTOMER_ID
