/******************************************************************
 *
 * File:    Chapter_5_RBAC_Demo.txt
 *
 * Purpose: Sample RBAC implementation worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

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
GRANT ROLE IDENTIFIER ( $test_load_role   ) TO USER <Your User Here>;
GRANT ROLE IDENTIFIER ( $test_owner_role  ) TO USER <Your User Here>;
GRANT ROLE IDENTIFIER ( $test_reader_role ) TO USER <Your User Here>;
GRANT ROLE IDENTIFIER ( $test_object_role ) TO USER <Your User Here>;
 

/*************************************/
/**** Setup Staging Owner Objects ****/
/*************************************/
USE ROLE      IDENTIFIER ( $test_object_role    );
USE DATABASE  IDENTIFIER ( $test_database       );
USE SCHEMA    IDENTIFIER ( $test_staging_schema );
USE WAREHOUSE IDENTIFIER ( $test_warehouse      );

-- Create table
CREATE OR REPLACE TABLE stg_test_load
(
id                                      STRING(255),
description                             STRING(255),
value                                   STRING(255),
stg_last_updated                        STRING(255)
);

GRANT SELECT, INSERT, TRUNCATE ON stg_test_load TO IDENTIFIER ( $test_load_role  );
GRANT SELECT, REFERENCES       ON stg_test_load TO IDENTIFIER ( $test_owner_role );

/*******************************************************/
/**** Use Owner Role to Create Owner Schema Objects ****/
/*******************************************************/
USE ROLE      IDENTIFIER ( $test_object_role  );  -- Object Owner Role
USE DATABASE  IDENTIFIER ( $test_database     );
USE SCHEMA    IDENTIFIER ( $test_owner_schema );  -- Owner Schema
USE WAREHOUSE IDENTIFIER ( $test_warehouse    );

CREATE SEQUENCE seq_test_load_id START WITH 100000;

SELECT * FROM TEST.staging_owner.stg_test_load;

CREATE OR REPLACE STREAM strm_stg_test_load ON TABLE TEST.staging_owner.stg_test_load;

-- No rows
SELECT * FROM strm_stg_test_load;

CREATE OR REPLACE TABLE int_test_load
(
test_load_id                            NUMBER,      -- Sequence generated surrogate Primary Key
id                                      NUMBER,      -- Supplied record ID
description                             STRING(255),
value                                   NUMBER,
stg_last_updated                        TIMESTAMP
);

CREATE OR REPLACE PROCEDURE sp_merge_test_load()
RETURNS string
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt  = "";
   var err_state = "";
   var result    = "";

   sql_stmt += "INSERT INTO TEST.test_owner.int_test_load\n"
   sql_stmt += "SELECT seq_test_load_id.NEXTVAL,         \n"
   sql_stmt += "       id,                               \n"
   sql_stmt += "       description,                      \n"
   sql_stmt += "       value,                            \n"
   sql_stmt += "       stg_last_updated                  \n"
   sql_stmt += "FROM   strm_stg_test_load;"

   stmt = snowflake.createStatement( { sqlText: sql_stmt } );

   try
   {
      stmt.execute();

      result = "Success";
   }
   catch(err)
   {
      err_state += "\nFail Code: "    + err.code;
      err_state += "\nState: "        + err.state;
      err_state += "\nMessage : "     + err.message;
      err_state += "\nStack Trace:\n" + err.StackTraceTxt;

      result = err_state;
   }

   return result;
$$;

/***************************************/
/**** Use Staging Role to Load Data ****/
/***************************************/
USE ROLE      IDENTIFIER ( $test_load_role      );  -- Inbound Data Role
USE DATABASE  IDENTIFIER ( $test_database       );
USE SCHEMA    IDENTIFIER ( $test_staging_schema );  -- Staging Schema
USE WAREHOUSE IDENTIFIER ( $test_warehouse      );

TRUNCATE TABLE stg_test_load;

INSERT INTO stg_test_load
VALUES
( 1000, 'Test Record 1','1','2021-12-08 16:39:11.700' ),
( 1001, 'Test Record 2','2','2021-12-08 16:39:11.700' );

SELECT * FROM stg_test_load;

/*********************************************/
/**** Revert to Owner Role to Ingest Data ****/
/*********************************************/
USE ROLE      IDENTIFIER ( $test_object_role  );  -- Object Owner Role
USE DATABASE  IDENTIFIER ( $test_database     );
USE SCHEMA    IDENTIFIER ( $test_owner_schema );  -- Owner Schema
USE WAREHOUSE IDENTIFIER ( $test_warehouse    );

SELECT * FROM strm_stg_test_load;

CALL sp_merge_test_load();

SELECT * FROM strm_stg_test_load;

SELECT * FROM int_test_load;

/**********************************************************************/
/**** Grant CREATE TASK to Object Owner Role for Owner Schema only ****/
/**********************************************************************/
USE ROLE securityadmin;

GRANT CREATE TASK ON SCHEMA IDENTIFIER ( $test_owner_schema   ) TO ROLE IDENTIFIER ( $test_object_role );

/*********************************************/
/**** Revert to Owner Role to Create Task ****/
/*********************************************/
USE ROLE      IDENTIFIER ( $test_object_role  );  -- Object Owner Role
USE DATABASE  IDENTIFIER ( $test_database     );
USE SCHEMA    IDENTIFIER ( $test_owner_schema );  -- Owner Schema
USE WAREHOUSE IDENTIFIER ( $test_warehouse    );

SELECT * FROM strm_stg_test_load;

SHOW streams;

SELECT system$stream_has_data( 'STRM_STG_TEST_LOAD' );

CREATE OR REPLACE TASK task_stg_test_load
WAREHOUSE = test_wh
SCHEDULE  = '1 minute'
WHEN system$stream_has_data ( 'STRM_STG_TEST_LOAD' )
AS
CALL sp_merge_test_load();

-- Check state
SHOW tasks;

/*************************************************/
/**** Grant EXECUTE TASK to Object Owner Role ****/
/*************************************************/
USE ROLE accountadmin;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE IDENTIFIER ( $test_object_role );

/*********************************************/
/**** Revert to Owner Role to Create Task ****/
/*********************************************/
USE ROLE      IDENTIFIER ( $test_object_role  );  -- Object Owner Role
USE DATABASE  IDENTIFIER ( $test_database     );
USE SCHEMA    IDENTIFIER ( $test_owner_schema );  -- Owner Schema
USE WAREHOUSE IDENTIFIER ( $test_warehouse    );

ALTER TASK task_stg_test_load RESUME;

SHOW tasks;

SELECT * FROM int_test_load;

/************************************************/
/**** Change to Owner Role and Output Schema ****/
/************************************************/
USE ROLE      IDENTIFIER ( $test_object_role   );  -- Object Owner Role
USE DATABASE  IDENTIFIER ( $test_database      );
USE SCHEMA    IDENTIFIER ( $test_reader_schema );  -- Output Schema
USE WAREHOUSE IDENTIFIER ( $test_warehouse     );

CREATE OR REPLACE SECURE VIEW v_secure_test_load COPY GRANTS
AS
SELECT * FROM TEST.test_owner.int_test_load;

SELECT * FROM v_secure_test_load;

GRANT SELECT ON v_secure_test_load TO ROLE IDENTIFIER ( $test_reader_role );


USE ROLE useradmin;

CREATE OR REPLACE USER test
PASSWORD             = 'test'
DISPLAY_NAME         = 'Test User'
EMAIL                = 'test@test.xyz'
DEFAULT_ROLE         = 'test_reader_role'
DEFAULT_NAMESPACE    = 'TEST.reader_owner'
DEFAULT_WAREHOUSE    = 'test_wh'
COMMENT              = 'Test user'
MUST_CHANGE_PASSWORD = FALSE;

USE ROLE securityadmin;

GRANT ROLE test_reader_role TO USER test;


/***************************************/
/**** Login using different browser ****/
/***************************************
USE ROLE      test_reader_role;
USE DATABASE  TEST;
USE WAREHOUSE test_wh;
USE SCHEMA    reader_owner;

SELECT * FROM TEST.reader_owner.v_secure_test_load;
****************************************/


/*************************/
/**** Troubleshooting ****/
/*************************/
SELECT current_user(), current_role();

SHOW GRANTS TO ROLE test_reader_role;


/**************************/
/**** Remove Test Case ****/
/**************************
USE ROLE sysadmin;

DROP DATABASE IDENTIFIER ( $test_database );

DROP WAREHOUSE IDENTIFIER ( $test_warehouse );
 
USE ROLE securityadmin;
 
DROP ROLE IDENTIFIER ( $test_load_role   );
DROP ROLE IDENTIFIER ( $test_owner_role  );
DROP ROLE IDENTIFIER ( $test_reader_role );
DROP ROLE IDENTIFIER ( $test_object_role );

USE ROLE useradmin;

DROP USER test;
**************************/


