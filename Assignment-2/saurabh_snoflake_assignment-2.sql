-- Assigment 2

-- In this project, we will learn how to use snowflake as a query engine. We store our data in aws s3 and we will learn various methods to query it from snowflake.

-- A. Query data in s3 from snowflake.
-- B. Create view over data in aws s3.
-- C. Disadvantages and advantages of this approach.

-- Using role sysadmin and compute warehouse we will create table with 100M records

use role sysadmin;
use warehouse compute_wh;
create or replace database demo_db;
CREATE OR REPLACE TRANSIENT TABLE DEMO_DB.PUBLIC.CUSTOMER_TEST AS SELECT * FROM "SNOWFLAKE_SAMPLE_DATA"."TPCDS_SF100TCL"."CUSTOMER";


-- Create external S3 stage
-- Using role accountadmin to create S3 storage integration object and grant ownership of the same to sysadmin role.

use role accountadmin;
CREATE or replace STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::533267294414:role/s3bucket-FullAccess'
  ENABLED = TRUE
  STORAGE_ALLOWED_LOCATIONS = ('s3://saurabhp31-snowflake/assignment2/');
DESC INTEGRATION s3_int;
show  integrations;
grant ownership on INTEGRATION s3_int to sysadmin;

--Create file format

use role sysadmin;
CREATE OR REPLACE FILE FORMAT my_csv_format
TYPE = CSV
COMPRESSION = gzip;

--Create Stage
CREATE OR REPLACE STAGE demo_db.public.my_s3_stage
  STORAGE_INTEGRATION = s3_int
  URL = 's3://saurabhp31-snowflake/assignment2//'
  FILE_FORMAT = my_csv_format;

-- Copy the table create in snowflake to external stage
copy into @demo_db.public.my_s3_stage from demo_db.public.customer_test;
list @demo_db.public.my_s3_stage;


-- Query the data from external stage

SELECT $1 C_CUSTOMER_SK,
$2 C_CUSTOMER_ID ,
$3 C_CURRENT_CDEMO_SK , 
$4 C_CURRENT_HDEMO_SK ,
$5 C_CURRENT_ADDR_SK , 
$6 C_FIRST_SHIPTO_DATE_SK ,
$7 C_FIRST_SALES_DATE_SK , 
$8 C_SALUTATION ,
$9 C_FIRST_NAME , 
$10 C_LAST_NAME,
$11 C_PREFERRED_CUST_FLAG , 
$12 C_BIRTH_DAY , 
$13 C_BIRTH_MONTH , 
$14 C_BIRTH_YEAR, 
$16 C_LOGIN , 
$17 C_EMAIL_ADDRESS , 
$18 C_LAST_REVIEW_DATE 
FROM @DEMO_DB.PUBLIC.MY_S3_STAGE (file_format =>  DEMO_DB.PUBLIC.my_csv_format);

-- Filter data directly from s3
SELECT $1 C_CUSTOMER_SK,
$2 C_CUSTOMER_ID ,
$3 C_CURRENT_CDEMO_SK , 
$4 C_CURRENT_HDEMO_SK ,
$5 C_CURRENT_ADDR_SK , 
$6 C_FIRST_SHIPTO_DATE_SK ,
$7 C_FIRST_SALES_DATE_SK , 
$8 C_SALUTATION ,
$9 C_FIRST_NAME , 
$10 C_LAST_NAME,
$11 C_PREFERRED_CUST_FLAG , 
$12 C_BIRTH_DAY , 
$13 C_BIRTH_MONTH , 
$14 C_BIRTH_YEAR,
$16 C_LOGIN , 
$17 C_EMAIL_ADDRESS , 
$18 C_LAST_REVIEW_DATE 
FROM @DEMO_DB.PUBLIC.MY_S3_STAGE (file_format =>  DEMO_DB.PUBLIC.my_csv_format)
WHERE C_CUSTOMER_SK ='64596949';

-- Execute group by,
SELECT $9 C_FIRST_NAME,
       $10 C_LAST_NAME,COUNT(*) 
FROM @DEMO_DB.PUBLIC.MY_S3_STAGE (file_format => DEMO_DB.PUBLIC.MY_CSV_FORMAT) 
GROUP BY $9,$10;

-- 4. CREATE VIEW OVER S3 DATA

CREATE OR REPLACE VIEW CUSTOMER_DATA_VIEW AS 
SELECT  $1 C_CUSTOMER_SK, 
        $2 C_CUSTOMER_ID , 
        $3 C_CURRENT_CDEMO_SK , 
        $4 C_CURRENT_HDEMO_SK , 
        $5 C_CURRENT_ADDR_SK , 
        $6 C_FIRST_SHIPTO_DATE_SK , 
        $7 C_FIRST_SALES_DATE_SK ,
        $8 C_SALUTATION , 
        $9 C_FIRST_NAME , 
        $10 C_LAST_NAME, 
        $11 C_PREFERRED_CUST_FLAG , 
        $12 C_BIRTH_DAY , 
        $13 C_BIRTH_MONTH ,  
        $14 C_BIRTH_YEAR, 
        $16 C_LOGIN , 
        $17 C_EMAIL_ADDRESS , 
        $18 C_LAST_REVIEW_DATE 
        FROM @DEMO_DB.PUBLIC.MY_S3_STAGE (file_format => DEMO_DB.PUBLIC.MY_CSV_FORMAT);


SELECT * FROM CUSTOMER_DATA_VIEW;

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- Q2). Now we can directly query data from s3 through view. What is the disadvantage of using this approach ? Can you see partitions being scanned in the backend ?
-- Query Performance: Since views on external stages read data directly from external sources, query performance can be impacted by network latency and the read performance of the external storage.
-- As this is normal view and not materialized the query will execute everytime we will run it. And external data will get scanned again and again.  
-- Transferring data between Snowflake and external stages can incur network egress charges, particularly if the data is frequently accessed or large in volume.
-- We CANNOT see any partition scanned in the back-end while querying on view. 


-- Now let’s try to Join the view we created with a table on snowflake,
-- Create Sample table that we will join it with view.
Create or replace transient table CUSTOMER_SNOWFLAKE_TABLE AS 
SELECT * FROM CUSTOMER_TEST limit 10000;

-- Joining Sample table with the view
SELECT B.* 
FROM CUSTOMER_SNOWFLAKE_TABLE B 
LEFT OUTER JOIN CUSTOMER_DATA_VIEW A 
ON A.C_CUSTOMER_SK = B.C_CUSTOMER_SK;
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Q3). Now we successfully joined data in s3 with snowflake table. It may look simple but this approach has lot of potential. Can you mention few below:
-- Here we are joining a view that is created using data/file in external stage with snowflake internal table. It provides advantages where we can utlize S3 storage which will be cost effective and have strong features for storage purpose. 
-- We can connect with cloud storage and join internal table without the need of loading the table directly in snowflake without running a seperate task to load the data in snowflake table and perform join. 
-- If new files are updated in S3 bucket we can perform the join operation on the updated files and again to automation pipe required to load new data. 
-- As it is a view, the end user doesn't know the  about file hosting or wheather it is which type of table it is.
 -----------------------------------------------------------------------------------------------------------------------------------------

 -- Q4). In the above example we joined a snowflake table with data on s3. Please check the profile page and observe the execution plan. How many partitions got scanned from snowflake
-- After observing the execution plan we found that there is external scan and internal scan. 
-- Most of the processing happened on external scan
-- One more filter operation got performed to cast customer key in numeric data type. 
-- The partion got scanned are 355 and total partion are 1. As scanned partion are more than the total partition we need optimize the query by doing effective partition.
-------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Q5). UNLOAD DATA BACK TO S3
--This approach leverages micro partitions in snowflake for lookup table still giving us the freedom to query data which we have stored in s3. Once we are done looking up we can copy data back to s3 with new derived lookup column.

COPY INTO @DEMO_DB.PUBLIC.MY_S3_STAGE/customer_join_table
from
( SELECT B.* 
  FROM CUSTOMER_SNOWFLAKE_TABLE B 
       LEFT OUTER JOIN CUSTOMER_DATA_VIEW A 
       ON A.C_CUSTOMER_SK = B.C_CUSTOMER_SK );
--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------

-- Q6). Advantages and Disadvantages.
-- ADVANTAGES 
--  Easy integration of new files
--  No neeed to load file continously in snowflake
--  We can design database schema where dimension table can be hosted on snowflake and fact table on cloud. We can create small lookup table by performing join operations. 
--  Considering storage it will be cost effective and additional security checks can be applied.
--  Data consitency can be achive as it is read only.

-- DISADVANTAGES
--  Transaction cost between cloud and snowflake
--  If data is in diiferent region it occur more cost. If there is access control issue we cannot fetch the files.
--  If there is policy update for role we need to re configure the integration
--  Compute cost is more as it is scanning more partition
--  All Snowflake features are not available. 



    













