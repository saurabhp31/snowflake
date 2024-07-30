
--create warehouse
use warehouse compute_wh;
 
--create database
create or replace database assgn3_db;
 
/* External Stage on S3:

a. Create User in AWS with Programmatic access and copy the credentials.
b. Create s3 bucket
c. Create Stage: Use below SQL statement in Snowflake to create external stage on s3(AWS)

*/
 
--Create external stage

CREATE OR REPLACE STAGE assgn_stg
URL='s3://assgn3/json/'
CREDENTIALS=(AWS_KEY_ID='AKIAXYKJVFDHMJ3Q****' AWS_SECRET_KEY='CvQYwnDlFSCnhqZ3agxiwJKxD8W/APuwYfKd****');
    -- aws_key_id(****)= CORP, AWS_SECRET_KEY(****)= 2gv9 

list @assgn_stg;
 
--CREATE table in Snowflake with VARIANT column to load data from stage with variant column
CREATE OR REPLACE TABLE person_source_data (source_data VARIANT);
 
 
--e. Create a Snowpipe with Auto Ingest Enabled

create or replace file format json_data
  type = 'json';

create or replace pipe person_snow_pipe 
auto_ingest=true 
as copy into person_sourcedata
from @customer.PUBLIC.S3_JSON
file_format=(format_name='json_data')
on_error = CONTINUE;

alter pipe person_snow_pipe refresh;
show pipes;
 

--generate code to enter in s3bucket event notoficatio
describe pipe customer.public.person_snow_pipe;

--to check pipe status
select system$pipe_status('customer.public.person_snow_pipe');

--to check copy history
select * from table (information_schema.
copy_history(table_name=>'person_sourcedata',
start_time=>dateadd(hours,-1, current_timestamp())));

-- Create a stream object
create or replace stream customer.public.person_stream on table customer.public.person_sourcedata;

select * from person_stream;
-- it will be empty as there is no file in stage yet

--Creating a table to Load the unnested data from the table person_sourcedata.
create or replace table customer.public.person_unnested(
  id int,
  name varchar,
  age number,
  location varchar,
  zip number);

--Creating a task  to load data from stream to person_unnested:
  CREATE OR REPLACE TASK customer.public.person_task
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = '1 MINUTE'
  when system$stream_has_data('customer.public.person_stream')
AS
 INSERT INTO customer.public.person_unnested
 select input_data:id::int, input_data:name::varchar, input_data:age::number, input_data:location::varchar, 
 input_data:zip::number
 from customer.public.person_sourcedata;


-- empty tables will be there. No file uploaded and task not executed
 select * from person_sourcedata;
 select * from person_stream;
 select * from person_unnested;

 Alter task person_task resume;
 --upload file in s3 bucket

 --data loaded in stage and then in person_sourcedata -> person_stream -> person_unnested


--procedure for loading data to different table
CREATE OR REPLACE PROCEDURE unnest_and_merge_person_data()
RETURNS STRING
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS
$$
try {
  // Define the SQL command for the merge operation
  var sql_command = `
    MERGE INTO person_sourcedata AS target
    USING (
      SELECT
        value:id::INTEGER AS id,
        value:Name::STRING AS name,
        value:age::INTEGER AS age,
        value:location::STRING AS location,
        value:zip::STRING AS zip
      FROM PERSON_NESTED,
      LATERAL FLATTEN(INPUT => person_unnested) AS flattened
    ) AS source
    ON target.id = source.id
    WHEN MATCHED THEN UPDATE SET
      target.name = source.name,
      target.age = source.age,
      target.location = source.location,
      target.zip = source.zip
    WHEN NOT MATCHED THEN INSERT (id, name, age, location, zip)
    VALUES (source.id, source.name, source.age, source.location, source.zip);
 
  // Create a statement object and execute the SQL command
  var stmt = snowflake.createStatement({sqlText: sql_command});
  stmt.execute();
 
  // Return success message
  return "Data unnest and merge completed successfully.";
} catch (err) {
  // Return error message if an exception occurs
  return "Failed to unnest and merge data: " + err.message;
}
$$;




 


 

 
  



 
 
 
 