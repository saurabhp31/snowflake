SELECT 
    c.CUSTOMER_ID,
    c.FIRST_NAME,
    c.LAST_NAME,
    cd.CREDIT_SCORE,
    cd.NUMBER_OF_LATE_PAYMENTS,
    cd.BANKRUPTCIES,
    GBLOAD_DB.GBLOAD.CD_RISK_LEVEL(CD.CREDIT_SCORE ,CD.BANKRUPTCIES ) AS CD_RISK_LEVEL
FROM {{ source('globalbank', 'customers') }} c
JOIN {{ source('globalbank', 'credit_data') }} cd ON c.CUSTOMER_ID = cd.CUSTOMER_ID
