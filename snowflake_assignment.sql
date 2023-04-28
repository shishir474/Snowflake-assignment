-- CREATE roles
CREATE OR REPLACE ROLE admin;
CREATE OR REPLACE ROLE developer;
CREATE OR REPLACE ROLE pii;

-- assigning ROLEs
GRANT ROLE admin
TO ROLE accountadmin;

GRANT ROLE developer
TO ROLE admin;

GRANT ROLE pii
TO ROLE accountadmin;

-- CREATE warehouse
USE ROLE accountadmin;


CREATE OR REPLACE warehouse assignment_wh
WITH warehouse_type = 'Standard'
warehouse_size=  'Medium'
auto_suspend = 300
min_cluster_count = 1
max_cluster_count = 3
comment = 'This warehouse is to be used for evaluating assignment queries';

USE warehouse assignment_wh;

GRANT usage ON warehouse assignment_wh TO ROLE admin;
GRANT usage ON warehouse assignment_wh TO ROLE developer;
GRANT usage ON warehouse assignment_wh TO ROLE pii;

GRANT CREATE DATABASE ON ACCOUNT TO admin;
USE ROLE admin;
CREATE or REPLACE database assignment_db;

GRANT usage ON database assignment_db TO ROLE admin;
GRANT usage ON database assignment_db TO ROLE developer;
GRANT usage ON database assignment_db TO ROLE pii;

CREATE or REPLACE schema assignment_db.my_schema;

GRANT usage ON schema assignment_db.my_schema TO ROLE admin;
GRANT usage ON schema assignment_db.my_schema TO ROLE developer;
GRANT usage ON schema assignment_db.my_schema TO ROLE pii;

GRANT SELECT ON TABLE assignment_db.my_schema.employees_ext TO ROLE developer;
GRANT SELECT ON TABLE assignment_db.my_schema.employees_ext TO ROLE pii;

CREATE OR REPLACE TABLE assignment_db.my_schema.employees_ext(
id int,
first_name string,
last_name string,
email string,
location string,
department STRING,
elt_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
elt_by STRING DEFAULT 'AWS',
filename STRING
);

CREATE OR REPLACE FILE FORMAT assignment_db.my_schema.csv_format
type = 'csv'
field_delimiter = ','
skip_header = 1;

CREATE OR REPLACE STORAGE INTEGRATION s3_int
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = 'S3'
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = 'arn:aws:iam::843032173205:role/snowflake_assignment_role'
  STORAGE_ALLOWED_LOCATIONS = ('s3://shishirsnowflakebucket/csv/');

DESC INTEGRATION s3_int;

CREATE OR REPLACE stage csv_stage
url = 's3://shishirsnowflakebucket/csv/'
FILE_FORMAT = assignment_db.my_schema.csv_format
STORAGE_INTEGRATION = s3_int;

List @csv_stage;
desc stage csv_stage;

-- copy FROM external stage
COPY INTO assignment_db.my_schema.employees_ext
FROM @csv_stage
files = ('employee_data_1.csv');

SELECT * FROM assignment_db.my_schema.employees_ext;


CREATE OR REPLACE TABLE assignment_db.my_schema.employees_int(
id int,
first_name string,
last_name string,
email string,
location string,
department string
);

-- load file into internal stage
-- put file://~/Desktop/employee_data_1.csv @int_stage;

COPY INTO assignment_db.my_schema.employees_int
FROM @int_stage;

SELECT * FROM assignment_db.my_schema.employees_int;

CREATE OR REPLACE TABLE assignment_db.my_schema.employees_var(
id int,
data variant
);

-- OBJECT_INSERT function to construct a JSON object fOR each row in the my_structured_TABLE 
INSERT INTO assignment_db.my_schema.employees_var
SELECT id, object_insert(object_insert(object_insert(object_insert(object_insert({}, 'first_name', first_name), 'last_name', last_name), 'email', email), 'location', location), 'department', department)
FROM assignment_db.my_schema.employees_int;

SELECT * FROM assignment_db.my_schema.employees_var;

--CREATE variant version of dataset
CREATE OR REPLACE FILE FORMAT parquet_format
type = 'parquet';

CREATE OR REPLACE stage parquet_stage
FILE_FORMAT = parquet_format;

-- put file:///users/shishirsingh/Desktop/userdata1.parquet @parquet_stage;

-- inferschema of the parquet file
SELECT * FROM TABLE(infer_schema(location=>'@parquet_stage',FILE_FORMAT=>'parquet_format'));

SELECT * FROM @parquet_stage;

SELECT $1:birthdate::string, $1:cc::string FROM @parquet_stage;



-- implementing masking policy
USE ROLE developer;
USE ROLE pii;

SELECT * FROM assignment_db.my_schema.employees_ext;

-- creating masking policy
CREATE OR REPLACE masking POLICY email
    AS (val varchar) returns varchar -> 
        CASE 
        WHEN current_role() IN ('ACCOUNTADMIN', 'PII') THEN val
        ELSE '##-###-##'
        END;

-- apply policy on a specific column
ALTER TABLE assignment_db.my_schema.employees_ext MODIFY column email
SET MASKING POLICY email;












