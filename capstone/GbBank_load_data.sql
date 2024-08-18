
 
------ creating the stage databases --------------
create or replace database GBSTAGE_DB;
use database GBSTAGE_DB;
 
 ------ creating the stage schema -----------------
create or replace schema GBSTAGE_DB.GBSTAGE;
use schema GBSTAGE;
 
------ create external storage integration --------
CREATE OR REPLACE STORAGE INTEGRATION capstone_csv
TYPE = EXTERNAL_STAGE
STORAGE_PROVIDER = S3
ENABLED = TRUE
STORAGE_AWS_ROLE_ARN ='arn:aws:iam::533267294414:role/s3bucket-FullAccess'
STORAGE_ALLOWED_LOCATIONS =('s3://saurabh-p31-snowflake/capstone/');
 
desc integration capstone_csv;

----- create file_format ---------------------
create or replace file format csv_file_format
type='csv'
FIELD_OPTIONALLY_ENCLOSED_BY = '"' 
FIELD_DELIMITER = ','
RECORD_DELIMITER = '\n'
SKIP_HEADER = 1;
 
------ Create external stage ------------------
CREATE OR REPLACE STAGE my_capstone
STORAGE_INTEGRATION = capstone_csv
URL = 's3://saurabh-p31-snowflake/capstone/'
file_format = csv_file_format;
 
 
------ creating the  stage tables ---------------------
CREATE TABLE GBSTAGE_DB.GBSTAGE.transactions (
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
);
 
CREATE TABLE GBSTAGE_DB.GBSTAGE.customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);
 
CREATE TABLE GBSTAGE_DB.GBSTAGE.accounts (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);
 
CREATE TABLE GBSTAGE_DB.GBSTAGE.credit_data (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);
 
CREATE TABLE GBSTAGE_DB.GBSTAGE.watchlist (
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);

---------- create streams on above table---------------------
CREATE OR REPLACE STREAM GBSTAGE_DB.GBSTAGE.transactions_stream ON TABLE GBSTAGE_DB.GBSTAGE.transactions;
CREATE OR REPLACE STREAM GBSTAGE_DB.GBSTAGE.accounts_stream ON TABLE GBSTAGE_DB.GBSTAGE.accounts;
CREATE OR REPLACE STREAM GBSTAGE_DB.GBSTAGE.credit_data_stream ON TABLE GBSTAGE_DB.GBSTAGE.credit_data;
CREATE OR REPLACE STREAM GBSTAGE_DB.GBSTAGE.customers_stream ON TABLE GBSTAGE_DB.GBSTAGE.customers;
CREATE OR REPLACE STREAM GBSTAGE_DB.GBSTAGE.watchlist_stream ON TABLE GBSTAGE_DB.GBSTAGE.watchlist;

 
 list @my_capstone; 
 
----- creating the pipes ------------

CREATE OR REPLACE PIPE transactions_pipe
auto_ingest = true AS
COPY INTO transactions
FROM @my_capstone/transactions.csv
FILE_FORMAT =  GBSTAGE_DB.GBSTAGE.CSV_FILE_FORMAT
ON_ERROR = CONTINUE;
 
desc pipe transactions_pipe;

CREATE OR REPLACE PIPE customers_pipe
auto_ingest = true AS
COPY INTO customers
FROM @my_capstone/customer.csv
FILE_FORMAT =  GBSTAGE_DB.GBSTAGE.CSV_FILE_FORMAT
ON_ERROR = CONTINUE;

desc pipe customers_pipe;
 
CREATE OR REPLACE PIPE accounts_pipe
auto_ingest = true AS
COPY INTO accounts
FROM @my_capstone/account.csv
FILE_FORMAT =  GBSTAGE_DB.GBSTAGE.CSV_FILE_FORMAT
ON_ERROR = CONTINUE;
 
desc pipe accounts_pipe;
 
CREATE OR REPLACE PIPE credit_data_pipe
auto_ingest = true AS
COPY INTO credit_data
FROM @my_capstone/credit.csv
FILE_FORMAT =  GBSTAGE_DB.GBSTAGE.CSV_FILE_FORMAT
ON_ERROR = CONTINUE;
 
desc pipe credit_data_pipe;
 
CREATE OR REPLACE PIPE watchlist_pipe
auto_ingest = true AS
COPY INTO watchlist
FROM @my_capstone/watchlist.csv
FILE_FORMAT =  GBSTAGE_DB.GBSTAGE.CSV_FILE_FORMAT
ON_ERROR = CONTINUE;
 
desc pipe watchlist_pipe;

show pipes;
 
------- to refresh pipes ---------
alter pipe transactions_pipe refresh;
alter pipe customers_pipe refresh;
alter pipe accounts_pipe refresh;
alter pipe credit_data_pipe refresh;
alter pipe watchlist_pipe refresh;

 --- check data in stream and stage table-------------------- 
select * from GBSTAGE_DB.GBSTAGE.transactions;
select * from GBSTAGE_DB.GBSTAGE.customers;
select * from GBSTAGE_DB.GBSTAGE.accounts;
select * from GBSTAGE_DB.GBSTAGE.credit_data;
select * from GBSTAGE_DB.GBSTAGE.watchlist;

select * from GBSTAGE_DB.GBSTAGE.transactions_stream;
select * from GBSTAGE_DB.GBSTAGE.customers_stream;
select * from GBSTAGE_DB.GBSTAGE.accounts_stream;
select * from GBSTAGE_DB.GBSTAGE.credit_data_stream;
select * from GBSTAGE_DB.GBSTAGE.watchlist_stream;

-----create load database----------------
create or replace database GBLOAD_DB;

-----create load schema----------------
create or replace schema GBLOAD_DB.GBLOAD;

------create load table--------------------
CREATE TABLE GBLOAD_DB.GBLOAD.transactions (
    transaction_id STRING,
    customer_id STRING,
    transaction_date TIMESTAMP_NTZ,
    amount FLOAT,
    currency STRING,
    transaction_type STRING,
    channel STRING,
    merchant_name STRING,
    merchant_category STRING,
    location_country STRING,
    location_city STRING,
    is_flagged BOOLEAN
);
 
CREATE TABLE GBLOAD_DB.GBLOAD.customers (
    customer_id STRING,
    first_name STRING,
    last_name STRING,
    date_of_birth DATE,
    gender STRING,
    email STRING,
    phone_number STRING,
    address STRING,
    city STRING,
    country STRING,
    occupation STRING,
    income_bracket STRING,
    customer_since DATE
);
 
CREATE TABLE GBLOAD_DB.GBLOAD.accounts (
    account_id STRING,
    customer_id STRING,
    account_type STRING,
    account_status STRING,
    open_date DATE,
    current_balance FLOAT,
    currency STRING,
    credit_limit FLOAT
);
 
CREATE TABLE GBLOAD_DB.GBLOAD.credit_data (
    customer_id STRING,
    credit_score INT,
    number_of_credit_accounts INT,
    total_credit_limit FLOAT,
    total_credit_used FLOAT,
    number_of_late_payments INT,
    bankruptcies INT
);
 
CREATE TABLE GBLOAD_DB.GBLOAD.watchlist (
    entity_id STRING,
    entity_name STRING,
    entity_type STRING,
    risk_category STRING,
    listed_date DATE,
    source STRING
);

------ create task to load data from stream ---------------
CREATE OR REPLACE TASK GBLOAD_DB.GBLOAD.transactions_tsk
WAREHOUSE = compute_wh 
SCHEDULE  = '1 minute'
WHEN system$stream_has_data('GBSTAGE_DB.GBSTAGE.transactions_stream')
AS
INSERT INTO GBLOAD_DB.GBLOAD.transactions
SELECT * FROM GBSTAGE_DB.GBSTAGE.transactions_stream;

CREATE OR REPLACE TASK GBLOAD_DB.GBLOAD.accounts_tsk
WAREHOUSE = compute_wh 
SCHEDULE  = '1 minute'
WHEN system$stream_has_data('GBSTAGE_DB.GBSTAGE.accounts_stream')
AS
INSERT INTO GBLOAD_DB.GBLOAD.accounts
SELECT * FROM GBSTAGE_DB.GBSTAGE.accounts_stream;


CREATE OR REPLACE TASK GBLOAD_DB.GBLOAD.credit_data_tsk
WAREHOUSE = compute_wh 
SCHEDULE  = '1 minute'
WHEN system$stream_has_data('GBSTAGE_DB.GBSTAGE.credit_data_stream')
AS
INSERT INTO GBLOAD_DB.GBLOAD.credit_data
SELECT * FROM GBSTAGE_DB.GBSTAGE.credit_data_stream;


CREATE OR REPLACE TASK GBLOAD_DB.GBLOAD.customers_tsk
WAREHOUSE = compute_wh 
SCHEDULE  = '1 minute'
WHEN system$stream_has_data('GBSTAGE_DB.GBSTAGE.customers_stream')
AS
INSERT INTO GBLOAD_DB.GBLOAD.customers
SELECT * FROM GBSTAGE_DB.GBSTAGE.customers_stream;


CREATE OR REPLACE TASK GBLOAD_DB.GBLOAD.watchlist_tsk
WAREHOUSE = compute_wh 
SCHEDULE  = '1 minute'
WHEN system$stream_has_data('GBSTAGE_DB.GBSTAGE.watchlist_stream')
AS
INSERT INTO GBLOAD_DB.GBLOAD.watchlist
SELECT * FROM GBSTAGE_DB.GBSTAGE.watchlist_stream;

-----------resume task------------------------------
ALTER TASK GBLOAD_DB.GBLOAD.transactions_tsk RESUME;
ALTER TASK GBLOAD_DB.GBLOAD.accounts_tsk RESUME;
ALTER TASK GBLOAD_DB.GBLOAD.credit_data_tsk RESUME;
ALTER TASK GBLOAD_DB.GBLOAD.customers_tsk RESUME;
ALTER TASK GBLOAD_DB.GBLOAD.watchlist_tsk RESUME;


--------check stream and load table data--------------
select * from GBSTAGE_DB.GBSTAGE.transactions_stream;
select * from GBSTAGE_DB.GBSTAGE.customers_stream;
select * from GBSTAGE_DB.GBSTAGE.accounts_stream;
select * from GBSTAGE_DB.GBSTAGE.credit_data_stream;
select * from GBSTAGE_DB.GBSTAGE.watchlist_stream;

select * from GBLOAD_DB.GBLOAD.transactions;
select * from GBLOAD_DB.GBLOAD.customers;
select * from GBLOAD_DB.GBLOAD.accounts;
select * from GBLOAD_DB.GBLOAD.credit_data;
select * from GBLOAD_DB.GBLOAD.watchlist;


----------------------------------------------------------------------------------------------


 
 

 
 
--select * from transaction ;
--select *, trans_func(amount) as risk_level from transaction;
 
 
REVOKE APPLYBUDGET ON DATABASE raw_db FROM ROLE PC_DBT_ROLE;
grant all privileges on DATABASE raw_db to role PC_DBT_ROLE;
grant all privileges on schema RAW_schema to role PC_DBT_ROLE;
grant select on all tables in schema RAW_schema to role PC_DBT_ROLE;
GRANT SELECT ON FUTURE TABLES IN DATABASE raw_db TO ROLE PC_DBT_ROLE;

 