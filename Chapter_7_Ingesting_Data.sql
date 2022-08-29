/******************************************************************
 *
 * File:    Chapter_7_Ingesting_Data.txt
 *
 * Purpose: Sample data ingestion worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

USE ROLE     sysadmin;
USE DATABASE TEST;
USE SCHEMA   public;

CREATE OR REPLACE FILE FORMAT TEST.public.test_pipe_format
TYPE                = CSV
FIELD_DELIMITER     = '|'
SKIP_HEADER         = 1
NULL_IF             = ( 'NULL', 'null' )
EMPTY_FIELD_AS_NULL = TRUE
SKIP_BLANK_LINES    = TRUE;

SHOW FILE FORMATS LIKE 'test_pipe_format';

USE ROLE     accountadmin;
USE DATABASE TEST;
USE SCHEMA   public;

CREATE OR REPLACE STORAGE INTEGRATION test_integration
TYPE                      = EXTERNAL_STAGE
STORAGE_PROVIDER          = S3
ENABLED                   = TRUE
STORAGE_AWS_ROLE_ARN      = 'arn:aws:iam::616701129608:role/test_role'
STORAGE_ALLOWED_LOCATIONS = ( 's3://btsdc-test-bucket/' );

GRANT USAGE ON INTEGRATION test_integration TO ROLE sysadmin;

DESC INTEGRATION test_integration;

USE ROLE      sysadmin;
USE DATABASE  TEST;
USE SCHEMA    public;

CREATE OR REPLACE STAGE TEST.public.test_stage
STORAGE_INTEGRATION = test_integration
DIRECTORY           = ( ENABLE = TRUE AUTO_REFRESH = TRUE )
ENCRYPTION          = ( TYPE = 'SNOWFLAKE_SSE' )
URL                 = 's3://btsdc-test-bucket/'
FILE_FORMAT         = TEST.public.test_pipe_format;

LIST @TEST.public.test_stage;

SELECT $1 FROM @TEST.public.test_stage;

USE ROLE securityadmin;

GRANT USAGE ON FILE FORMAT TEST.public.test_pipe_format TO ROLE test_object_role;
GRANT USAGE ON STAGE       TEST.public.test_stage       TO ROLE test_object_role;
GRANT USAGE ON SCHEMA      TEST.public                  TO ROLE test_object_role;

USE ROLE      IDENTIFIER ( $test_object_role    );
USE DATABASE  IDENTIFIER ( $test_database       );
USE SCHEMA    IDENTIFIER ( $test_staging_schema );
USE WAREHOUSE IDENTIFIER ( $test_warehouse      );

SELECT * FROM TEST.test_owner.int_test_load;

LIST @TEST.public.test_stage;

COPY INTO @TEST.public.test_stage/int_test_load FROM TEST.test_owner.int_test_load;

LIST @TEST.public.test_stage;

USE ROLE      sysadmin;
USE DATABASE  TEST;
USE SCHEMA    public;

CREATE OR REPLACE STAGE TEST.public.named_stage
DIRECTORY           = ( ENABLE = TRUE )
ENCRYPTION          = ( TYPE = 'SNOWFLAKE_SSE' )
FILE_FORMAT         = TEST.public.test_pipe_format;

LIST @TEST.public.named_stage;

USE ROLE      sysadmin;
USE DATABASE  TEST;
USE SCHEMA    public;

PUT file:///a:/a.sql @TEST.public.named_stage;

LIST @TEST.public.named_stage;

PUT file:///a:/a.sql @TEST.public.named_stage auto_compress=FALSE;

SELECT * FROM information_schema.stages;

REMOVE @TEST.public.named_stage;

USE ROLE      sysadmin;
USE DATABASE  TEST;
USE SCHEMA    public;

CREATE OR REPLACE TABLE csv_test
(
id     NUMBER,
label  VARCHAR(30)
);


PUT file://a:\a.csv @%CSV_TEST auto_compress=FALSE;

COPY INTO csv_test FROM @%CSV_TEST;

SELECT * FROM csv_test;

REMOVE @%CSV_TEST;

PUT 'file:///a.csv' '@"TEST"."PUBLIC".%"CSV_TEST"/ui1641748114927';

COPY INTO "TEST"."PUBLIC"."CSV_TEST" FROM @/ui1641748114927 FILE_FORMAT = '"TEST"."PUBLIC"."TEST_PIPE_FORMAT"' ON_ERROR = 'ABORT_STATEMENT' PURGE = TRUE;

LIST @~;

PUT file://a:\a.csv @~/dummy auto_compress=FALSE;

LIST @~;
LIST @~/dummy;

/**************************/
/**** Remove Test Case ****/
/**************************
USE ROLE      sysadmin;
USE DATABASE  TEST;
USE SCHEMA    public;

DROP STAGE       TEST.public.test_stage;
DROP STAGE       TEST.public.named_stage;
DROP FILE FORMAT TEST.public.test_pipe_format
DROP TABLE       csv_test;

USE ROLE     accountadmin;
DROP STORAGE INTEGRATION test_integration;
*/
