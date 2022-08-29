/******************************************************************
 *
 * File:    Chapter_10_Semi_Structured_and_Unstructured.txt
 *
 * Purpose: Sample semi-structured and unstructured worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

/***************************/
/* Semi Structured Support */
/***************************/
USE ROLE     accountadmin;
USE DATABASE TEST;
USE SCHEMA   public;

CREATE OR REPLACE STORAGE INTEGRATION json_integration
TYPE                      = EXTERNAL_STAGE
STORAGE_PROVIDER          = S3
ENABLED                   = TRUE
STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::616701129608:role/test_role'
STORAGE_ALLOWED_LOCATIONS = ( 's3://btsdc-json-bucket/' );

USE ROLE securityadmin;

GRANT USAGE ON INTEGRATION json_integration TO ROLE sysadmin;

DESC INTEGRATION json_integration;

USE ROLE      sysadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    public;

CREATE OR REPLACE STAGE TEST.public.json_stage
STORAGE_INTEGRATION = json_integration
DIRECTORY           = ( ENABLE = TRUE AUTO_REFRESH = TRUE )
ENCRYPTION          = ( TYPE = 'SNOWFLAKE_SSE' )
URL                 = 's3://btsdc-json-bucket/'
FILE_FORMAT         = TEST.public.test_pipe_format;

DESC STAGE TEST.public.json_stage;

SELECT "property_value"
FROM   TABLE ( RESULT_SCAN ( last_query_id()))
WHERE  "property" = 'DIRECTORY_NOTIFICATION_CHANNEL';

SELECT $1 FROM @TEST.public.json_stage/json_test.json;

SELECT * FROM DIRECTORY ( @TEST.public.json_stage );

ALTER STAGE TEST.public.json_stage REFRESH;

CREATE OR REPLACE TABLE stg_json_test
(
json_test   VARIANT
);

COPY INTO stg_json_test FROM @TEST.public.json_stage/json_test.json
FILE_FORMAT = test_json_format;

SELECT * FROM stg_json_test;

SELECT json_test:employee[0].firstName::STRING,
       json_test:employee[1].firstName::STRING
FROM   stg_json_test;

SELECT e.value:firstName::STRING
FROM   stg_json_test,
       LATERAL FLATTEN ( json_test:employee ) e;

SELECT e.value:firstName::STRING,
       e.value:lastName::STRING,
       e.value:gender::STRING,
       e.value:socialSecurityNumber::STRING,
       e.value:dateOfBirth::STRING,
       e.value:address.streetAddress::STRING,
       e.value:address.city::STRING,
       e.value:address.state::STRING,
       e.value:address.postalCode::STRING,
       p.value:type::STRING,
       p.value:number::STRING
FROM   stg_json_test,
       LATERAL FLATTEN ( json_test:employee )                  e,
       LATERAL FLATTEN ( e.value:phoneNumbers, OUTER => TRUE ) p;

CREATE OR REPLACE MATERIALIZED VIEW mv_stg_json_test
AS
SELECT e.value:firstName::STRING              AS first_name,
       e.value:lastName::STRING               AS last_name,
       e.value:gender::STRING                 AS gender,
       e.value:socialSecurityNumber::STRING   AS social_security_number,
       e.value:dateOfBirth::STRING            AS date_of_birth,
       e.value:address.streetAddress::STRING  AS street_address,
       e.value:address.city::STRING           AS city,
       e.value:address.state::STRING          AS state,
       e.value:address.postalCode::STRING     AS post_code,
       p.value:type::STRING                   AS phone_type,
       p.value:number::STRING                 AS phone_number
FROM   stg_json_test,
       LATERAL FLATTEN ( json_test:employee )                  e,
       LATERAL FLATTEN ( e.value:phoneNumbers, OUTER => TRUE ) p;

SHOW MATERIALIZED VIEWS;

SELECT * FROM mv_stg_json_test;

/* Automation */
LIST @TEST.public.json_stage;

SELECT * FROM DIRECTORY ( @TEST.public.json_stage );

/* Delete then load file */
ALTER STAGE TEST.public.json_stage REFRESH;

CREATE OR REPLACE STREAM strm_test_public_json_stage ON STAGE TEST.public.json_stage;

SELECT * FROM strm_test_public_json_stage;

CREATE OR REPLACE SECURE VIEW v_strm_test_public_json_stage
AS
SELECT '@TEST.public.json_stage'                         AS stage_name,
       get_stage_location    ( @TEST.public.json_stage ) AS stage_location,
       relative_path,
       get_absolute_path     ( @TEST.public.json_stage,
                             relative_path )             AS absolute_path,
       get_presigned_url     ( @TEST.public.json_stage,
                             relative_path )             AS presigned_url,
       build_scoped_file_url ( @TEST.public.json_stage,
                             relative_path )             AS scoped_file_url,
       build_stage_file_url  ( @TEST.public.json_stage,
                             relative_path )             AS stage_file_url,
       size,
       last_modified,
       md5,
       etag,
       file_url,
       metadata$action,
       metadata$isupdate
FROM   strm_test_public_json_stage;

SELECT * FROM v_strm_test_public_json_stage;

CREATE OR REPLACE TASK task_load_json_data
WAREHOUSE = COMPUTE_WH
SCHEDULE  = '1 minute'
WHEN system$stream_has_data ( 'strm_test_public_json_stage' )
AS
CALL sp_load_json_data();

/****************/
/* Unstructured */
/****************/
ALTER STAGE TEST.public.json_stage REFRESH;

SELECT * FROM v_strm_test_public_json_stage;

LIST @TEST.public.json_stage;

SELECT $1 FROM @TEST.public.json_stage;

SELECT $1 FROM @TEST.public.json_stage/<your_invoice_here>.png.json;


USE ROLE accountadmin;

SELECT system$get_privatelink_config();

CREATE OR REPLACE API INTEGRATION document_integration
API_PROVIDER         = aws_api_gateway
API_AWS_ROLE_ARN     = 'arn:aws:iam::616701129608:role/Snowflake_External_Function'
//API_KEY              = 'uqkOKvci6OajRNCYXvohX9WTY8HpxGzt5vj9eDHV'
ENABLED              = TRUE
API_ALLOWED_PREFIXES = ('https://8qdfb0w8fh.execute-api.eu-west-2.amazonaws.com/test/snowflake_proxy');

DESC INTEGRATION document_integration;

/* Update trust relationship */

CREATE OR REPLACE EXTERNAL FUNCTION get_image_data ( n INTEGER, v VARCHAR )
RETURNS VARIANT
API_INTEGRATION = document_integration
AS 'https://8qdfb0w8fh.execute-api.eu-west-2.amazonaws.com/test/snowflake_proxy';

SELECT get_image_data ( 1, 'name' );

GRANT USAGE ON FUNCTION get_image_data(INTEGER, VARCHAR) TO ROLE sysadmin;

/* Modify lambda */

USE ROLE      sysadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    public;

SELECT get_image_data ( 1, 'name' );

ALTER STAGE TEST.public.json_stage REFRESH;

SELECT * FROM DIRECTORY ( @TEST.public.json_stage );

SELECT * FROM v_strm_test_public_json_stage;

SELECT *
FROM   v_strm_test_public_json_stage
WHERE  relative_path = '<your_invoice_here>.png';

SELECT presigned_url
FROM   v_strm_test_public_json_stage
WHERE  relative_path = '<your_invoice_here>.png';

SELECT get_presigned_url(@TEST.public.json_stage, get_relative_path(@TEST.public.json_stage, 's3://btsdc-json-bucket/<your_invoice_here>.png'));

SELECT get_image_data(0, get_presigned_url(@TEST.public.json_stage, get_relative_path(@TEST.public.json_stage, 's3://btsdc-json-bucket/<your_invoice_here>.png')));

SELECT parse_json(get_image_data(0, get_presigned_url(@TEST.public.json_stage, relative_path))):"<your_string>"::string
FROM   v_strm_test_public_json_stage
WHERE  relative_path = '<your_invoice_here>.png';

