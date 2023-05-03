
Task 1:
Created three roles (Admin, Developer, PII) and defined their hierarchy

Task 2:
Created a medium sized warehouse
Granted all priveleges on warehouse to all roles

Task 3:
Switched to admin role

Task 4:
Created a database
Granted all priveleges on database to all roles

Task 5:
Created a schema
Granted all priveleges on schema to all roles

Task 6: 
Created a new table within the my_schema schema called employee_ext
Granted all priveleges on table to all roles.

Task 7:
Created a variant table with id(int) and data(variant) columns
Added data into this table from employee_ext table using object_insert

Task 8:
Created a new storage integration called s3_init with an S3 storage provider
Created an external stage named csv_stage
Created an internal stage named int_stage
Created a table named employees_int

Task 9:
Copied data from external stage to employees_ext table
Copied data from internal stage to employees_int table

Task 10:
Created a file format named parquet_format of type parquet
Created a stage named parquet_stage
Then infered its schema using this file format

Task 11:
Ran a query on parquet file stored in stage

Task 12:
Created a masking policy and applied it in email column

