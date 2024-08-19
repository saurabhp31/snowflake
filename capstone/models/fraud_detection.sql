SELECT 
    t.TRANSACTION_ID,
    t.CUSTOMER_ID,
    t.TRANSACTION_DATE,
    t.AMOUNT,
    t.MERCHANT_NAME,
    t.LOCATION_COUNTRY,
    CASE 
        WHEN t.AMOUNT > 5000 THEN 'High Value'
        WHEN t.MERCHANT_NAME IN (SELECT ENTITY_NAME FROM {{ source('globalbank', 'watchlist') }}) THEN 'Watchlist Merchant'
        WHEN t.IS_FLAGGED = TRUE THEN 'Flagged Transaction'
        ELSE 'Normal'
    END AS FRAUD_RISK
FROM {{ source('globalbank', 'transactions') }} t
