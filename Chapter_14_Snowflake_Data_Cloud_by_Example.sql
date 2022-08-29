/******************************************************************
 *
 * File:    Chapter_14_Snowflake_Data_Cloud.txt
 *
 * Purpose: Sample Data Sharing implementation worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

/****************************** Start of TEST Setup ******************************/

/**********************/
/**** Declarations ****/
/**********************/
SET test_database       = 'TEST';
SET test_staging_schema = 'TEST.staging_owner';
SET test_owner_schema   = 'TEST.test_owner';
SET test_reader_schema  = 'TEST.reader_owner';
SET test_warehouse      = 'test_wh';
SET test_load_role      = 'test_load_role';
SET test_owner_role     = 'test_owner_role';
SET test_reader_role    = 'test_reader_role';
SET test_object_role    = 'test_object_role';

/****************************************************/
/**** Create TEST database warehouse and schemas ****/
/****************************************************/
USE ROLE sysadmin;
 
CREATE OR REPLACE DATABASE IDENTIFIER ( $test_database ) DATA_RETENTION_TIME_IN_DAYS = 90;

CREATE OR REPLACE WAREHOUSE IDENTIFIER ( $test_warehouse ) WITH
WAREHOUSE_SIZE      = 'X-SMALL'
AUTO_SUSPEND        = 60
AUTO_RESUME         = TRUE
MIN_CLUSTER_COUNT   = 1
MAX_CLUSTER_COUNT   = 4
SCALING_POLICY      = 'STANDARD'
INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE SCHEMA IDENTIFIER ( $test_staging_schema );
CREATE OR REPLACE SCHEMA IDENTIFIER ( $test_owner_schema   );
CREATE OR REPLACE SCHEMA IDENTIFIER ( $test_reader_schema  );
 
/**********************/
/**** Create Roles ****/
/**********************/
USE ROLE securityadmin;

CREATE OR REPLACE ROLE IDENTIFIER ( $test_load_role   )       COMMENT = 'TEST.test_load Role';
CREATE OR REPLACE ROLE IDENTIFIER ( $test_owner_role  )       COMMENT = 'TEST.test_owner Role';
CREATE OR REPLACE ROLE IDENTIFIER ( $test_reader_role )       COMMENT = 'TEST.test_reader Role';
CREATE OR REPLACE ROLE IDENTIFIER ( $test_object_role )       COMMENT = 'TEST.test_object Role';

GRANT ROLE IDENTIFIER ( $test_load_role   ) TO ROLE securityadmin;
GRANT ROLE IDENTIFIER ( $test_owner_role  ) TO ROLE securityadmin;
GRANT ROLE IDENTIFIER ( $test_reader_role ) TO ROLE securityadmin;
GRANT ROLE IDENTIFIER ( $test_object_role ) TO ROLE securityadmin;

/********************************/
/**** Create GRANTs to roles ****/
/********************************/
GRANT USAGE   ON DATABASE  IDENTIFIER ( $test_database       ) TO ROLE IDENTIFIER ( $test_load_role   );
GRANT USAGE   ON WAREHOUSE IDENTIFIER ( $test_warehouse      ) TO ROLE IDENTIFIER ( $test_load_role   );
GRANT OPERATE ON WAREHOUSE IDENTIFIER ( $test_warehouse      ) TO ROLE IDENTIFIER ( $test_load_role   );
GRANT USAGE   ON SCHEMA    IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_load_role   );

GRANT USAGE   ON DATABASE  IDENTIFIER ( $test_database       ) TO ROLE IDENTIFIER ( $test_owner_role  );
GRANT USAGE   ON WAREHOUSE IDENTIFIER ( $test_warehouse      ) TO ROLE IDENTIFIER ( $test_owner_role  );
GRANT OPERATE ON WAREHOUSE IDENTIFIER ( $test_warehouse      ) TO ROLE IDENTIFIER ( $test_owner_role  );
GRANT USAGE   ON SCHEMA    IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_owner_role  );

GRANT USAGE   ON DATABASE  IDENTIFIER ( $test_database       ) TO ROLE IDENTIFIER ( $test_reader_role );
GRANT USAGE   ON WAREHOUSE IDENTIFIER ( $test_warehouse      ) TO ROLE IDENTIFIER ( $test_reader_role );
GRANT OPERATE ON WAREHOUSE IDENTIFIER ( $test_warehouse      ) TO ROLE IDENTIFIER ( $test_reader_role );
GRANT USAGE   ON SCHEMA    IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_reader_role );

GRANT USAGE   ON DATABASE  IDENTIFIER ( $test_database       ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT USAGE   ON WAREHOUSE IDENTIFIER ( $test_warehouse      ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT OPERATE ON WAREHOUSE IDENTIFIER ( $test_warehouse      ) TO ROLE IDENTIFIER ( $test_object_role );


GRANT USAGE                      ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT MONITOR                    ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT MODIFY                     ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE TABLE               ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE VIEW                ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE SEQUENCE            ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE FUNCTION            ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE PROCEDURE           ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE STREAM              ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE MATERIALIZED VIEW   ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE FILE FORMAT         ON SCHEMA IDENTIFIER ( $test_staging_schema ) TO ROLE IDENTIFIER ( $test_object_role );

GRANT USAGE                      ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT MONITOR                    ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT MODIFY                     ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE TABLE               ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE VIEW                ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE SEQUENCE            ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE FUNCTION            ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE PROCEDURE           ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE STREAM              ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE MATERIALIZED VIEW   ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE FILE FORMAT         ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );

GRANT USAGE                      ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT MONITOR                    ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT MODIFY                     ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE TABLE               ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE VIEW                ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE SEQUENCE            ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE FUNCTION            ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE PROCEDURE           ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE STREAM              ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE MATERIALIZED VIEW   ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );
GRANT CREATE FILE FORMAT         ON SCHEMA IDENTIFIER ( $test_reader_schema  ) TO ROLE IDENTIFIER ( $test_object_role );


/**********************************/
/**** Assign to logged in user ****/
/**********************************/
GRANT ROLE IDENTIFIER ( $test_load_role   ) TO USER <your_user>;
GRANT ROLE IDENTIFIER ( $test_owner_role  ) TO USER <your_user>;
GRANT ROLE IDENTIFIER ( $test_reader_role ) TO USER <your_user>;
GRANT ROLE IDENTIFIER ( $test_object_role ) TO USER <your_user>;

/******************************* End of TEST Setup *******************************/

/****************/
/* Data Sharing */
/****************/
USE ROLE      accountadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    public;

CREATE OR REPLACE SHARE btsdc_share;

USE ROLE securityadmin;

GRANT USAGE ON DATABASE TEST            TO SHARE btsdc_share;
GRANT USAGE ON SCHEMA   TEST.test_owner TO SHARE btsdc_share;

SHOW shares;

USE ROLE      test_object_role;
USE DATABASE  TEST;
USE SCHEMA    TEST.test_owner;

CREATE OR REPLACE TABLE csv_test
(
id     NUMBER,
label  VARCHAR(30)
);

USE ROLE securityadmin;

GRANT SELECT ON TABLE TEST.test_owner.csv_test TO SHARE btsdc_share;

SHOW GRANTS TO SHARE btsdc_share;

USE ROLE accountadmin;

ALTER SHARE btsdc_share SET
ACCOUNTS = <your_consumer_account>,
COMMENT  = 'Test share to Account xxxx';

ALTER SHARE btsdc_share REMOVE ACCOUNT = <your_consumer_account>;

USE ROLE      test_object_role;
USE DATABASE  TEST;
USE SCHEMA    TEST.test_owner;

CREATE OR REPLACE TABLE csv_test_2
(
id     NUMBER,
label  VARCHAR(30)
);

USE ROLE securityadmin;

GRANT SELECT ON TABLE TEST.test_owner.csv_test_2 TO SHARE btsdc_share;

SHOW GRANTS TO SHARE btsdc_share;

SELECT current_account();


/************************************/
/* Import Data Share - 2nd Account! */
/************************************/
USE ROLE accountadmin;

CREATE OR REPLACE DATABASE btsdc_import_share
FROM SHARE WN03315.btsdc_share
COMMENT = 'Imported btsdc_share';

USE ROLE securityadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE btsdc_import_share TO ROLE sysadmin;

USE ROLE sysadmin;

SHOW SCHEMAS IN DATABASE btsdc_import_share;

SHOW TABLES IN SCHEMA btsdc_import_share.test_owner;

SELECT current_account();


USE ROLE securityadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE sysadmin;

USE ROLE sysadmin;

SELECT * FROM btsdc_import_share.test_owner.csv_test;


/**********************************************/
/**** Enable access to Account Usage Store ****/
/**********************************************/
USE ROLE securityadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE test_object_role;

USE ROLE      test_object_role;
USE WAREHOUSE TEST_WH;
USE DATABASE  TEST;
USE SCHEMA    TEST.test_owner;

INSERT INTO csv_test VALUES
(1, 'aaa'),
(2, 'bbb'),
(3, 'ccc');

CREATE OR REPLACE SECURE VIEW v_access_history AS
SELECT qh.query_text,
       qh.user_name||' -> '||qh.role_name||' -> '||qh.warehouse_name  AS user_info,
       qh.database_name||'.'||qh.schema_name||' -> '||qh.query_type||' -> '||qh.execution_status AS object_query,
       ah.query_start_time,
       ah.direct_objects_accessed,
       ah.base_objects_accessed,
       ah.objects_modified,
       ah.query_id
FROM   snowflake.account_usage.access_history ah,
       snowflake.account_usage.query_history  qh
WHERE  ah.query_id                            = qh.query_id
ORDER BY ah.query_start_time DESC;


/*******************************/
/* Centralised Cost Monitoring */
/*******************************/
USE ROLE accountadmin;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE test_object_role;

USE ROLE securityadmin;

GRANT CREATE TASK ON SCHEMA TEST.test_owner TO ROLE test_object_role;

USE ROLE      test_object_role;
USE WAREHOUSE TEST_WH;
USE DATABASE  TEST;
USE SCHEMA    TEST.test_owner;

CREATE OR REPLACE SECURE VIEW v_warehouse_spend COPY GRANTS
AS
SELECT wmh.warehouse_name,
       SUM ( wmh.credits_used )             AS credits_used,
       EXTRACT ( 'YEAR',  wmh.start_time )||
       EXTRACT ( 'MONTH', wmh.start_time )||
       EXTRACT ( 'DAY',   wmh.start_time )  AS spend_date
FROM   snowflake.account_usage.warehouse_metering_history wmh
WHERE  TO_DATE ( spend_date, 'YYYYMMDD' ) = current_date() -1
GROUP BY wmh.warehouse_name,
         spend_date
ORDER BY spend_date DESC, wmh.warehouse_name ASC;

SELECT * FROM v_warehouse_spend;

CREATE OR REPLACE TABLE warehouse_spend_hist
(
warehouse_name VARCHAR,
credits_used   NUMBER(18,6),
spend_date     VARCHAR,
last_updated   TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL
);

CREATE OR REPLACE TASK task_load_warehouse_spend_hist
WAREHOUSE = TEST_WH
SCHEDULE  = 'USING CRON * * * * * UTC'
--SCHEDULE  = 'USING CRON 0 1 * * * UTC'
AS
INSERT INTO warehouse_spend_hist
SELECT warehouse_name,
       credits_used,
       spend_date,
       current_timestamp()::TIMESTAMP_NTZ
FROM   v_warehouse_spend;

SHOW tasks;

ALTER TASK task_load_warehouse_spend_hist RESUME;

SELECT timestampdiff ( second, current_timestamp, scheduled_time ) as next_run,
       scheduled_time,
       current_timestamp,
       name,
       state
FROM   TABLE ( information_schema.task_history())
WHERE  state = 'SCHEDULED'
ORDER BY completed_time DESC;

ALTER TASK task_load_warehouse_spend_hist SUSPEND;


CREATE OR REPLACE SECURE FUNCTION fn_get_warehouse_spend()
RETURNS TABLE ( warehouse_name VARCHAR,
                credits_used   NUMBER(18,6),
                spend_date     VARCHAR,
                last_updated   TIMESTAMP )
AS
$$
   SELECT warehouse_name,
          credits_used,
          spend_date,
          last_updated
   FROM   warehouse_spend_hist
$$
;

SELECT warehouse_name,
       credits_used,
       spend_date,
       last_updated
FROM   TABLE ( fn_get_warehouse_spend());


USE ROLE securityadmin;

GRANT USAGE ON FUNCTION TEST.test_owner.fn_get_warehouse_spend() TO SHARE btsdc_share;

SHOW GRANTS TO SHARE btsdc_share;


/*************************************/
/* Setup 2nd Account Report Database */
/*************************************/
USE ROLE sysadmin;

CREATE OR REPLACE DATABASE report DATA_RETENTION_TIME_IN_DAYS = 90;
CREATE OR REPLACE SCHEMA   report.report_owner;

USE ROLE securityadmin;

CREATE OR REPLACE ROLE report_owner_role;

GRANT ROLE report_owner_role TO ROLE securityadmin;

GRANT USAGE   ON DATABASE  report              TO ROLE report_owner_role;
GRANT USAGE   ON WAREHOUSE compute_wh          TO ROLE report_owner_role;
GRANT OPERATE ON WAREHOUSE compute_wh          TO ROLE report_owner_role;
GRANT USAGE   ON SCHEMA    report.report_owner TO ROLE report_owner_role;

GRANT CREATE TABLE             ON SCHEMA report.report_owner TO ROLE report_owner_role;
GRANT CREATE VIEW              ON SCHEMA report.report_owner TO ROLE report_owner_role;
GRANT CREATE SEQUENCE          ON SCHEMA report.report_owner TO ROLE report_owner_role;
GRANT CREATE STREAM            ON SCHEMA report.report_owner TO ROLE report_owner_role;
GRANT CREATE MATERIALIZED VIEW ON SCHEMA report.report_owner TO ROLE report_owner_role;

GRANT IMPORTED PRIVILEGES ON DATABASE btsdc_import_share TO ROLE report_owner_role;


/***********************************/
/* Use 2nd Account Report Database */
/***********************************/
USE ROLE      report_owner_role;
USE DATABASE  report;
USE WAREHOUSE compute_wh;
USE SCHEMA    report.report_owner;

SELECT 'Provider Account A' AS source_account,
       warehouse_name,
       credits_used,
       spend_date,
       last_updated                       AS source_last_updated,
       current_timestamp()::TIMESTAMP_NTZ AS insert_timestamp
FROM   TABLE ( btsdc_import_share.test_owner.fn_get_warehouse_spend());

CREATE OR REPLACE TABLE source_warehouse_spend_hist
(
source_account      VARCHAR(255),
warehouse_name      VARCHAR(255),
credits_used        NUMBER(18,6),
spend_date          VARCHAR(10),
source_last_updated TIMESTAMP_NTZ,
insert_timestamp    TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL
);

/* UDTF fn_get_warehouse_spend */
CREATE OR REPLACE SECURE VIEW v_source_warehouse_spend COPY GRANTS
AS
SELECT 'Provider Account A' AS source_account,
       warehouse_name,
       credits_used,
       spend_date,
       last_updated                       AS source_last_updated,
       current_timestamp()::TIMESTAMP_NTZ AS insert_timestamp
FROM   TABLE ( btsdc_import_share.test_owner.fn_get_warehouse_spend());

INSERT INTO source_warehouse_spend_hist
SELECT * FROM v_source_warehouse_spend;

