-- Using Role Accountadmin
use role accountadmin;

-- Create Databse Chess for futher tasks
create or replace database CHESS;

-- Make a file format
create or replace file format CHESS.public.json_data
type = 'json'
strip_outer_array=true;

-- Create AWS S3 external stage
CREATE OR REPLACE STAGE CHESS.PUBLIC.S3_JSON
URL='s3://saurabh-p31-snowflake/assignment4-chess/'
CREDENTIALS=(AWS_KEY_ID='AKIAXYKJVFDHMJ3Q****' AWS_SECRET_KEY='CvQYwnDlFSCnhqZ3agxiwJKxD8W/APuwYfKd****')
file_format=json_data ;

-- list the content of stage
list @s3_json;

-- Create source list table to load json data this will act as stage table list table
CREATE OR REPLACE TABLE list_source(v variant);
-- Create stream on above table
CREATE or REPLACE STREAM list_source_stream ON TABLE list_source;

-- Create source info table to load json data this will act as stage table for info table
CREATE OR REPLACE TABLE info_source (v variant);
-- Create stream on above tableCREATE or REPLACE STREAM info_source_stream  ON TABLE info_source;

-- Create source stats table to load json data this will act as stage table for stats table
CREATE OR REPLACE TABLE stats_source(v variant);
-- Create stream on above table
CREATE or REPLACE STREAM stats_source_stream ON TABLE stats_source;


-- pipe creation to copy data from list_file.json to list_source stage table
CREATE or REPLACE pipe chess_list_snowipe 
auto_ingest=true 
as copy into list_source
from @CHESS.PUBLIC.S3_JSON/list_file.json
file_format=(format_name='json_data')
on_error = CONTINUE;

-- describe pipe to genetate SQS code to insert in AWS event notifiation for trigger pipe in snowflake
desc chess_list_snowipe ;

-- pipe creation to copy data from info_file.json to info_source stage table
create or replace pipe chess_info_snowipe 
auto_ingest=true 
as copy into info_source
from @CHESS.PUBLIC.S3_JSON/info_file.json
file_format=(format_name='json_data')
on_error = CONTINUE;

-- describe pipe to genetate SQS code to insert in AWS event notifiation for trigger pipe in snowflake
desc chess_list_snowipe ;

-- pipe creation to copy data from stats_file.json to stats_source stage table
create or replace pipe chess_stats_snowipe 
auto_ingest=true 
as copy into stats_source
from @CHESS.PUBLIC.S3_JSON/stats_file.json
file_format=(format_name='json_data')
on_error = CONTINUE;

-- describe pipe to genetate SQS code to insert in AWS event notifiation for trigger pipe in snowflake
desc chess_list_snowipe ;

-- create temporary list table to load data from stream. This table will help to flatten data using lateral flatten.
CREATE OR REPLACE TABLE list_temp(v variant);

-- create temporary info table to load data from stream. This table will help to flatten data using lateral flatten. 
CREATE OR REPLACE TABLE info_temp (v variant);

-- create temporary stats table to load data from stream. This table will help to flatten data using lateral flatten. 
CREATE OR REPLACE TABLE stats_temp(v variant);

-- create task to load data from stream to temporary table 
CREATE or replace TASK process_list
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS  
INSERT INTO list_temp
SELECT parse_json($1)
FROM list_source_stream;

-- create task to load data from stream to temporary table 
CREATE or replace TASK process_info
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS  
INSERT INTO info_temp
SELECT parse_json($1)
FROM info_source_stream;

-- create task to load data from stream to temporary table 
CREATE or replace TASK process_stats
WAREHOUSE = COMPUTE_WH
SCHEDULE = '1 MINUTE'
AS  
INSERT INTO stats_temp
SELECT parse_json($1)
FROM stats_source_stream;

ALTER TASK process_list RESUME;
ALTER TASK process_info RESUME;
ALTER TASK process_stats RESUME;


-- create list table with required schema
CREATE OR REPLACE TABLE list_table (username varchar, is_live boolean);

-- create info table with required schema
CREATE OR REPLACE TABLE info_table 
(username varchar, followers numeric, country varchar, joined date, location varchar, name varchar, player_id numeric, status varchar, 
title varchar, primary_key numeric );

-- create stats table with required schema
CREATE OR REPLACE TABLE stats_table
(last_blitz numeric, draw_blitz numeric, loss_blitz numeric, win_blitz numeric, last_bullet numeric, draw_bullet numeric, loss_bullet numeric, win_bullet numeric, last_rapid numeric, draw_rapid numeric, loss_rapid numeric, win_rapid numeric, fide numeric, primary_key  numeric);

-- Insert json data to list table
insert into list_table
select PARSE_JSON($1):username, 
       PARSE_JSON($1):is_live 
from list_temp;

-- insert json data to info table
insert into info_table
select PARSE_JSON($1):username::varchar ,
    PARSE_JSON($1):followers::numeric, 
    PARSE_JSON($1):country::varchar, 
    PARSE_JSON($1):joined::date, 
    PARSE_JSON($1):location::varchar, 
    PARSE_JSON($1):name::varchar, 
    PARSE_JSON($1):player_id::numeric,
    PARSE_JSON($1):status::varchar, 
    PARSE_JSON($1):title::varchar, 
    PARSE_JSON($1):primary_key::numeric
from info_temp

-- insert into stats table
insert into stats_table
select cbz.value:last.rating as last_blitz::numeric, 
       cbz.value:record.draw as draw_blitz::numeric, 
       cbz.value:record.loss as loss_blitz::numeric, 
       cbz.value:record.win as win_blitz::numeric, 
       cbt.value:last.rating as last_bullet::numeric, 
       cbt.value:record.draw as draw_bullet::numeric, 
       cbt.value:record.loss as loss_bullet::numeric, 
       cbt.value:record.win as win_bullet::numeric, 
       cbr.value:last.rating as last_rapid::numeric, 
       cbr.value:record.draw as draw_rapid::numeric, 
       cbr.value:record.loss as loss_rapid::numeric, 
       cbr.value:record.win as win_rapid::numeric, 
       v.value:fide as fide::numeric, 
       v.key as username::varchar
from stats_temp 
        inner join lateral flatten(input => v) v
        inner join lateral flatten(input => v.value:chess_blitz) cbz
        inner join lateral flatten(input => v.value:chess_bullet) cbt
        inner join lateral flatten(input => v.value:chess_rapid) crd
        
---------------------------------------------------------------------------------------------------------------------------------------------------

-- Username of the best player by category (blitz, chess, bullet)

create table best_blitz as (
select info_table.username, stats_table.last_blitz as best_blitz
from stats_table join info_table
on stats_table.username= info_table.username
where last_blitz = (
    select max(last_blitz) from stats_table));

create table best_bullet as (
select info_table.username, stats_table.last_bullet as best_bullet
from stats_table join info_table
on stats_table.username = info_table.username
where last_bullet = (
    select max(last_bullet) from stats_table));

create table best_rapid as (
select info_table.username, stats_table.last_rapid as best_rapid
from stats_table join info_table
on stats_table.username = info_table.username
where last_rapid = (
    select max(last_rapid) from stats_table));
    
select *
from best_blitz join best_bullet join best_rapid;


--------------------------------------------------------------------------------------------------------------------------------------------------
-- full name or username of best player by country

select
    ifnull(info_table.name, info_table.username) as "Name",
    best.best_blitz,
    stats_table.FIDE
from info_table
    join stats_table 
        on info_table.username = stats_table.username
    join (
      select max(stats_table.last_blitz) as best_blitz
      from stats_table join info_table
      on stats_table.username = info_table.username) as best
        on stats_table.last_blitz = best.best_blitz
order by best.best_blitz desc;



------------------------------------------------------------------------------------------------------------------------------------------------------ 
-- avg elo by status (premium, staff, basic)

select info_table.status,
round(avg(stats_table.last_blitz),2) as average_blitz,
round(avg(stats_table.last_bullet),2) as average_bullet,
round(avg(stats_table.last_rapid),2) as average_rapid,
count(info_table.country) as number_of_streamers
from info_table right join stats_table
on info_table.primary_key = stats_table.primary_key
group by info_table.status
having average_blitz is not null
or average_bullet is not null
or average_rapid is not null
order by number_of_streamers desc, average_blitz desc, average_bullet desc, average_rapid desc;

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Number of professional players and their elo

select count(info_table.title)
from info_table;

select info_table.username, info_table.title, stats_table.FIDE
from stats_table join info_table
on stats_table.primary_key = info_table.primary_key
where stats_table.FIDE is not null
order by stats_table.FIDE desc;

--------------------------------------------------------------------------------------------------------------------------------------------------------
-- Average FIDE elo by their professional FIDE elo 

select info_table.title, avg(stats_table.FIDE) as average_FIDE
from stats_table join info_table
on stats_table.username = info_table.username
where stats_table.FIDE is not null and (info_table.title is not null)
group by info_table.title
order by average_FIDE desc;

----------------------------------------------------------------------------------------------------------------------------------------------------------
-- Best player currently on live 

select info_table.username, stats_table.last_blitz, stats_table.last_bullet, stats_table.last_rapid
from info_table
join stats_table
on info_table.primary_key = stats_table.primary_key
join list_table
on lower(info_table.username) = lower(list_table.username)
where list_table.is_live = TRUE and (last_blitz is not null or last_bullet is not null or last_rapid is not null)
order by last_blitz desc, last_bullet desc, last_rapid desc;

--------------------------------------------------------------------------------------------------------------------------------------------------------



































