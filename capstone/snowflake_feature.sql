REVOKE APPLYBUDGET ON DATABASE GBLOAD_DB FROM ROLE PC_DBT_ROLE;
grant all privileges on DATABASE GBLOAD_DB to role PC_DBT_ROLE;
grant all privileges on schema GBLOAD_DB.GBLOAD to role PC_DBT_ROLE;
grant select on all tables in schema GBLOAD_DB.GBLOAD to role PC_DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE GBLOAD_DB TO ROLE PC_DBT_ROLE;

REVOKE APPLYBUDGET ON DATABASE PC_DBT_DB FROM ROLE PC_DBT_ROLE;
grant all privileges on DATABASE PC_DBT_DB to role PC_DBT_ROLE;

create or replace function GBLOAD_DB.GBLOAD.AMOUNT_RISK_LEVEL(amount number)
returns string
language sql 
as
$$
select case when amount > 500 then 'High'
        when amount between 100 and 200 then 'Medium'
        else 'Low' end
 
$$
;

create or replace function GBLOAD_DB.GBLOAD.CD_RISK_LEVEL(CREDIT_SCORE number,BANKRUPTCIES NUMBER)
returns string
language sql 
as
$$
 SELECT CASE 
        WHEN CREDIT_SCORE < 600 OR BANKRUPTCIES > 0 THEN 'High Risk'
        WHEN CREDIT_SCORE BETWEEN 600 AND 700 THEN 'Medium Risk'
        ELSE 'Low Risk'
    END 
 
$$
;

grant USAGE on FUNCTION GBLOAD_DB.GBLOAD.CD_RISK_LEVEL(NUMBER ,NUMBER ) to role PC_DBT_ROLE;

CREATE MASKING POLICY GBLOAD_DB.GBLOAD.EMAIL_MASK AS
(EMAIL VARCHAR) RETURNS VARCHAR ->
CASE WHEN CURRENT_ROLE = 'ADMIN' THEN EMAIL
ELSE REGEXP_REPLACE(EMAIL, '.+\@', '*****@')
END;
 
ALTER TABLE GBLOAD_DB.GBLOAD.CUSTOMER MODIFY COLUMN email SET MASKING POLICY GBLOAD_DB.GBLOAD.;
 
 
CREATE MASKING POLICY GBLOAD_DB.GBLOAD.CUSTOMER AS
(PHONE VARCHAR) RETURNS VARCHAR ->
CASE WHEN CURRENT_ROLE = 'ADMIN' THEN PHONE
ELSE SUBSTR(PHONE, 0, 5) || '***-****'
END;
  
ALTER TABLE GBLOAD_DB.GBLOAD.CUSTOMER MODIFY COLUMN phone_number SET MASKING POLICY GBLOAD_DB.GBLOAD.Phone_MASK;
 
CREATE OR REPLACE MASKING POLICY GBLOAD_DB.GBLOAD.customer_id_MASK AS
(Cust_id VARCHAR) RETURNS VARCHAR ->
CASE
WHEN CURRENT_ROLE() = 'ADMIN' THEN Cust_id
ELSE 'XXXXXX'
END;

ALTER TABLE GBLOAD_DB.GBLOAD.CUSTOMER MODIFY COLUMN CUSTOMER_ID SET MASKING POLICY GBLOAD_DB.GBLOAD.customer_id_MASK;










 
