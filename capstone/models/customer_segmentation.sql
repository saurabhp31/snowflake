SELECT 
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    c.OCCUPATION,
    c.INCOME_BRACKET,
    COUNT(t.TRANSACTION_ID) AS TOTAL_TRANSACTIONS,
    SUM(t.AMOUNT) AS TOTAL_SPENT,
    CASE 
        WHEN SUM(t.AMOUNT) > 10000 THEN 'High Spender'
        WHEN SUM(t.AMOUNT) BETWEEN 5000 AND 10000 THEN 'Medium Spender'
        ELSE 'Low Spender'
    END AS SPENDING_CATEGORY
FROM {{ source('globalbank', 'customers') }} c
LEFT JOIN {{ source('globalbank', 'transactions') }} t ON c.CUSTOMER_ID = t.CUSTOMER_ID
GROUP BY c.CUSTOMER_ID, c.FIRST_NAME, c.LAST_NAME, c.OCCUPATION, c.INCOME_BRACKET
