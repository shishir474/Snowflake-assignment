-- create roles
create or replace role admin;
create or replace role developer;
create or replace role pii;

-- assigning roles
grant role admin
to role accountadmin;

grant role developer
to role admin;

grant role pii
to role accountadmin;

-- create warehouse
use role accountadmin;


create or replace warehouse assignment_wh
with warehouse_type = 'Standard'
warehouse_size=  'Medium'
auto_suspend = 300
min_cluster_count = 1
max_cluster_count = 3
comment = 'This warehouse is to be used for evaluating assignment queries';

use warehouse assignment_wh;

grant usage on warehouse assignment_wh to role admin;
grant usage on warehouse assignment_wh to role developer;
grant usage on warehouse assignment_wh to role pii;

create or replace database assignment_db;

grant usage on database assignment_db to role admin;
grant usage on database assignment_db to role developer;
grant usage on database assignment_db to role pii;

create or replace schema assignment_db.my_schema;

grant usage on schema assignment_db.my_schema to role admin;
grant usage on schema assignment_db.my_schema to role developer;
grant usage on schema assignment_db.my_schema to role pii;

Grant select on table assignment_db.my_schema.employees_ext to role developer;
Grant select on table assignment_db.my_schema.employees_ext to role pii;

create or replace table assignment_db.my_schema.employees_ext(
id int,
first_name string,
last_name string,
email string,
location string,
department string
);

create or replace file format assignment_db.my_schema.csv_format
type = 'csv'
field_delimiter = ','
skip_header = 1;

CREATE or replace STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::843032173205:role/snowflake_assignment_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://shishirsnowflakebucket/csv/');

DESC INTEGRATION s3_int;

create or replace stage csv_stage
url = 's3://shishirsnowflakebucket/csv/'
file_format = assignment_db.my_schema.csv_format
STORAGE_INTEGRATION = s3_int;

List @csv_stage;
desc stage csv_stage;

-- copy from external stage
copy into assignment_db.my_schema.employees_ext
from @csv_stage
files = ('employee_data_1.csv');

select * from assignment_db.my_schema.employees_ext;


create or replace table assignment_db.my_schema.employees_int(
id int,
first_name string,
last_name string,
email string,
location string,
department string
);

-- load file into internal stage
-- put file://~/Desktop/employee_data_1.csv @int_stage;

copy into assignment_db.my_schema.employees_int
from @int_stage;

select * from assignment_db.my_schema.employees_int;

create or replace table assignment_db.my_schema.employees_var(
id int,
data variant
);

-- OBJECT_INSERT function to construct a JSON object for each row in the my_structured_table 
insert into assignment_db.my_schema.employees_var
select id, object_insert(object_insert(object_insert(object_insert(object_insert({}, 'first_name', first_name), 'last_name', last_name), 'email', email), 'location', location), 'department', department)
from assignment_db.my_schema.employees_int;

select * from assignment_db.my_schema.employees_var;

--create variant version of dataset
create or replace file format parquet_format
type = 'parquet';

create or replace stage parquet_stage
file_format = parquet_format;

-- put file:///Users/shishirsingh/Desktop/userdata1.parquet @parquet_stage;

-- inferschema of the parquet file
select * from table(infer_schema(location=>'@parquet_stage',file_format=>'parquet_format'));

select * from @parquet_stage;

select $1:birthdate::string, $1:cc::string from @parquet_stage;



-- implementing masking policy
use role developer;
use role pii;

select * from assignment_db.my_schema.employees_ext;

-- creating masking policy
create or replace masking policy email
    as (val varchar) returns varchar -> 
        case 
        when current_role() in ('ACCOUNTADMIN', 'PII') then val
        else '##-###-##'
        end;

-- apply policy on a specific column
alter table assignment_db.my_schema.employees_ext modify column email
set masking policy email;












