/******************************************************************
 *
 * File:    Chapter_6_Account_Usage_Demo.txt
 *
 * Purpose: Sample Account Usage implementation worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

/**********************/
/**** Declarations ****/
/**********************/
SET monitor_database         = 'MONITOR';
SET monitor_reference_schema = 'MONITOR.reference_owner';
SET monitor_owner_schema     = 'MONITOR.monitor_owner';
SET monitor_warehouse        = 'monitor_wh';
SET monitor_reference_role   = 'monitor_reference_role';
SET monitor_owner_role       = 'monitor_owner_role';
SET monitor_reader_role      = 'monitor_reader_role';

/*******************************************************/
/**** Create MONITOR database warehouse and schemas ****/
/*******************************************************/
USE ROLE sysadmin;
 
CREATE OR REPLACE DATABASE IDENTIFIER ( $monitor_database ) DATA_RETENTION_TIME_IN_DAYS = 90;

CREATE OR REPLACE WAREHOUSE IDENTIFIER ( $monitor_warehouse ) WITH
WAREHOUSE_SIZE      = 'X-SMALL'
AUTO_SUSPEND        = 60
AUTO_RESUME         = TRUE
MIN_CLUSTER_COUNT   = 1
MAX_CLUSTER_COUNT   = 4
SCALING_POLICY      = 'STANDARD'
INITIALLY_SUSPENDED = TRUE;

CREATE OR REPLACE SCHEMA IDENTIFIER ( $monitor_reference_schema );
CREATE OR REPLACE SCHEMA IDENTIFIER ( $monitor_owner_schema     );
 
/**********************/
/**** Create Roles ****/
/**********************/
USE ROLE securityadmin;

CREATE OR REPLACE ROLE IDENTIFIER ( $monitor_reference_role ) COMMENT = 'MONITOR.monitor_reference Role';
CREATE OR REPLACE ROLE IDENTIFIER ( $monitor_owner_role     ) COMMENT = 'MONITOR.monitor_owner Role';
CREATE OR REPLACE ROLE IDENTIFIER ( $monitor_reader_role    ) COMMENT = 'MONITOR.monitor_reader Role';

GRANT ROLE IDENTIFIER ( $monitor_reference_role ) TO ROLE securityadmin;
GRANT ROLE IDENTIFIER ( $monitor_owner_role     ) TO ROLE securityadmin;
GRANT ROLE IDENTIFIER ( $monitor_reader_role    ) TO ROLE securityadmin;

/********************************/
/**** Create GRANTs to roles ****/
/********************************/
GRANT USAGE   ON DATABASE  IDENTIFIER ( $monitor_database         ) TO ROLE IDENTIFIER ( $monitor_reference_role  );
GRANT USAGE   ON WAREHOUSE IDENTIFIER ( $monitor_warehouse        ) TO ROLE IDENTIFIER ( $monitor_reference_role  );
GRANT OPERATE ON WAREHOUSE IDENTIFIER ( $monitor_warehouse        ) TO ROLE IDENTIFIER ( $monitor_reference_role  );
GRANT USAGE   ON SCHEMA    IDENTIFIER ( $monitor_reference_schema ) TO ROLE IDENTIFIER ( $monitor_reference_role  );

GRANT USAGE   ON DATABASE  IDENTIFIER ( $monitor_database         ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT USAGE   ON WAREHOUSE IDENTIFIER ( $monitor_warehouse        ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT OPERATE ON WAREHOUSE IDENTIFIER ( $monitor_warehouse        ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT USAGE   ON SCHEMA    IDENTIFIER ( $monitor_reference_schema ) TO ROLE IDENTIFIER ( $monitor_owner_role  );
GRANT USAGE   ON SCHEMA    IDENTIFIER ( $monitor_owner_schema     ) TO ROLE IDENTIFIER ( $monitor_owner_role  );

GRANT USAGE   ON DATABASE  IDENTIFIER ( $monitor_database         ) TO ROLE IDENTIFIER ( $monitor_reader_role );
GRANT USAGE   ON WAREHOUSE IDENTIFIER ( $monitor_warehouse        ) TO ROLE IDENTIFIER ( $monitor_reader_role );
GRANT OPERATE ON WAREHOUSE IDENTIFIER ( $monitor_warehouse        ) TO ROLE IDENTIFIER ( $monitor_reader_role );
GRANT USAGE   ON SCHEMA    IDENTIFIER ( $monitor_owner_schema     ) TO ROLE IDENTIFIER ( $monitor_reader_role );


GRANT CREATE TABLE             ON SCHEMA IDENTIFIER ( $monitor_reference_schema ) TO ROLE IDENTIFIER ( $monitor_reference_role );
GRANT CREATE VIEW              ON SCHEMA IDENTIFIER ( $monitor_reference_schema ) TO ROLE IDENTIFIER ( $monitor_reference_role );
GRANT CREATE SEQUENCE          ON SCHEMA IDENTIFIER ( $monitor_reference_schema ) TO ROLE IDENTIFIER ( $monitor_reference_role );
GRANT CREATE STREAM            ON SCHEMA IDENTIFIER ( $monitor_reference_schema ) TO ROLE IDENTIFIER ( $monitor_reference_role );
GRANT CREATE MATERIALIZED VIEW ON SCHEMA IDENTIFIER ( $monitor_reference_schema ) TO ROLE IDENTIFIER ( $monitor_reference_role );

GRANT CREATE TABLE               ON SCHEMA IDENTIFIER ( $monitor_owner_schema   ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT CREATE VIEW                ON SCHEMA IDENTIFIER ( $monitor_owner_schema   ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT CREATE SEQUENCE            ON SCHEMA IDENTIFIER ( $monitor_owner_schema   ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT CREATE FUNCTION            ON SCHEMA IDENTIFIER ( $monitor_owner_schema   ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT CREATE PROCEDURE           ON SCHEMA IDENTIFIER ( $monitor_owner_schema   ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT CREATE STREAM              ON SCHEMA IDENTIFIER ( $monitor_owner_schema   ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT CREATE MATERIALIZED VIEW   ON SCHEMA IDENTIFIER ( $monitor_owner_schema   ) TO ROLE IDENTIFIER ( $monitor_owner_role );
GRANT CREATE FILE FORMAT         ON SCHEMA IDENTIFIER ( $monitor_owner_schema   ) TO ROLE IDENTIFIER ( $monitor_owner_role );


/**********************************/
/**** Assign to logged in user ****/
/**********************************/
GRANT ROLE IDENTIFIER ( $monitor_reference_role ) TO USER <Your User Here>;
GRANT ROLE IDENTIFIER ( $monitor_owner_role     ) TO USER <Your User Here>;
GRANT ROLE IDENTIFIER ( $monitor_reader_role    ) TO USER <Your User Here>;
 

/**********************************************/
/**** Enable access to Account Usage Store ****/
/**********************************************/
GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE IDENTIFIER ( $monitor_owner_role );

/*************************************************************/
/**** Change to Owner Role and access Account Usage Store ****/
/*************************************************************/
USE ROLE      IDENTIFIER ( $monitor_owner_role   );
USE DATABASE  IDENTIFIER ( $monitor_database     );
USE SCHEMA    IDENTIFIER ( $monitor_owner_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse    );

SELECT * FROM SNOWFLAKE.account_usage.databases WHERE deleted IS NULL;

/***************************************/
/**** Setup Reference Owner Objects ****/
/***************************************/
USE ROLE      IDENTIFIER ( $monitor_reference_role   );
USE DATABASE  IDENTIFIER ( $monitor_database         );
USE SCHEMA    IDENTIFIER ( $monitor_reference_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse        );

-- Create Sequence
CREATE OR REPLACE SEQUENCE monitor_group_id_seq START WITH 10000;

-- Create tables
CREATE OR REPLACE TABLE monitor_group
(
monitor_group_id        NUMBER(10) PRIMARY KEY NOT NULL,
monitor_group_name      VARCHAR(255)           NOT NULL,
last_updated            TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL,
CONSTRAINT monitor_group_u1 UNIQUE ( monitor_group_name )
);

CREATE OR REPLACE TABLE hist_monitor_group
(
monitor_group_id        NUMBER(10)             NOT NULL,
monitor_group_name      VARCHAR(255)           NOT NULL,
valid_from              TIMESTAMP_NTZ,
valid_to                TIMESTAMP_NTZ,
current_flag            VARCHAR(1)
);

-- Create Stream
CREATE OR REPLACE STREAM strm_monitor_group ON TABLE monitor_group;

-- Create historization View
CREATE OR REPLACE VIEW v_monitor_group
AS
SELECT monitor_group_id,
       monitor_group_name,
       valid_from,
       valid_to,
       current_flag,
       'I' AS dml_type
FROM   (
       SELECT monitor_group_id,
              monitor_group_name,
              last_updated        AS valid_from,
              LAG ( last_updated ) OVER ( PARTITION BY monitor_group_id ORDER BY last_updated DESC ) AS valid_to_raw,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN '9999-12-31'::TIMESTAMP_NTZ
                    ELSE valid_to_raw
              END AS valid_to,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN 'Y'
                    ELSE 'N'
              END AS current_flag,
              'I' AS dml_type
       FROM   (
              SELECT strm.monitor_group_id,
                     strm.monitor_group_name,
                     strm.last_updated
              FROM   strm_monitor_group     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'FALSE'
              )	
       )
UNION ALL
SELECT monitor_group_id,
       monitor_group_name,
       valid_from,
       valid_to,
       current_flag,
       dml_type
FROM   (
       SELECT monitor_group_id,
              monitor_group_name,
              valid_from,
              LAG ( valid_from ) OVER ( PARTITION BY monitor_group_id ORDER BY valid_from DESC ) AS valid_to_raw,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN '9999-12-31'::TIMESTAMP_NTZ
                    ELSE valid_to_raw
              END AS valid_to,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN 'Y'
                    ELSE 'N'
              END AS current_flag,
              dml_type
       FROM   (
              SELECT strm.monitor_group_id,
                     strm.monitor_group_name,
                     strm.last_updated AS valid_from,
                     'I' AS dml_type
              FROM   strm_monitor_group     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'TRUE'
              UNION ALL
              SELECT tgt.monitor_group_id,
                     tgt.monitor_group_name,
                     tgt.valid_from,
                     'U' AS dml_type
              FROM   hist_monitor_group tgt
              WHERE  tgt.monitor_group_id IN
                     (
                     SELECT DISTINCT strm.monitor_group_id
                     FROM   strm_monitor_group     strm
                     WHERE  strm.metadata$action   = 'INSERT'
                     AND    strm.metadata$isupdate = 'TRUE'
                     )
              AND    tgt.current_flag = 'Y'
              )	
       )
UNION ALL
SELECT strm.monitor_group_id,
       strm.monitor_group_name,
       tgt.valid_from,
       current_timestamp()::TIMESTAMP_NTZ AS valid_to,
       NULL,
       'D' AS dml_type
FROM   hist_monitor_group tgt
INNER JOIN strm_monitor_group strm
   ON  tgt.monitor_group_id   = strm.monitor_group_id
WHERE  strm.metadata$action   = 'DELETE'
AND    strm.metadata$isupdate = 'FALSE'
AND    tgt.current_flag       = 'Y';

/*******************************/
/**** Create Monitor Groups ****/
/*******************************/
INSERT INTO monitor_group ( monitor_group_id, monitor_group_name ) VALUES ( monitor_group_id_seq.NEXTVAL, 'ACCOUNT PARAMETER' );
INSERT INTO monitor_group ( monitor_group_id, monitor_group_name ) VALUES ( monitor_group_id_seq.NEXTVAL, 'NETWORK POLICY' );
INSERT INTO monitor_group ( monitor_group_id, monitor_group_name ) VALUES ( monitor_group_id_seq.NEXTVAL, 'TIME TRAVEL' );
INSERT INTO monitor_group ( monitor_group_id, monitor_group_name ) VALUES ( monitor_group_id_seq.NEXTVAL, 'REST_EVENT_HISTORY' );

/**************************************/
/**** Historize Monitor Group Data ****/
/**************************************/
MERGE INTO hist_monitor_group tgt
USING v_monitor_group strm
ON    tgt.monitor_group_id   = strm.monitor_group_id
AND   tgt.valid_from         = strm.valid_from
AND   tgt.monitor_group_name = strm.monitor_group_name
WHEN MATCHED AND strm.dml_type = 'U' THEN
UPDATE
SET   tgt.valid_to     = strm.valid_to,
      tgt.current_flag = 'N'
WHEN MATCHED AND strm.dml_type = 'D' THEN
UPDATE
SET   tgt.valid_to     = strm.valid_to,
      tgt.current_flag = 'N'
WHEN NOT MATCHED AND strm.dml_type = 'I' THEN
INSERT
(
tgt.monitor_group_id,
tgt.monitor_group_name,
tgt.valid_from,
tgt.valid_to,
tgt.current_flag
)
VALUES
(
strm.monitor_group_id,
strm.monitor_group_name,
current_timestamp(),
strm.valid_to,
strm.current_flag
);


/********************************/
/**** Create Monitor Control ****/
/********************************/

-- Create Sequence
CREATE OR REPLACE SEQUENCE monitor_control_id_seq START WITH 10000;

-- Create tables
CREATE OR REPLACE TABLE monitor_control
(
monitor_control_id      NUMBER(10) PRIMARY KEY NOT NULL,
monitor_group_id        NUMBER(10)             NOT NULL REFERENCES monitor_group (monitor_group_id ),
monitor_control_name    VARCHAR(255)           NOT NULL,
last_updated            TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL,
CONSTRAINT monitor_control_u1 UNIQUE ( monitor_control_name )
);

CREATE OR REPLACE TABLE hist_monitor_control
(
monitor_control_id      NUMBER(10)             NOT NULL,
monitor_group_id        NUMBER(10) PRIMARY KEY NOT NULL,
monitor_control_name    VARCHAR(255)           NOT NULL,
valid_from              TIMESTAMP_NTZ,
valid_to                TIMESTAMP_NTZ,
current_flag            VARCHAR(1)
);

-- Create Stream
CREATE OR REPLACE STREAM strm_monitor_control ON TABLE monitor_control;

-- Create historization View
CREATE OR REPLACE VIEW v_monitor_control
AS
SELECT monitor_control_id,
       monitor_group_id,
       monitor_control_name,
       valid_from,
       valid_to,
       current_flag,
       'I' AS dml_type
FROM   (
       SELECT monitor_control_id,
              monitor_group_id,
              monitor_control_name,
              last_updated        AS valid_from,
              LAG ( last_updated ) OVER ( PARTITION BY monitor_control_id ORDER BY last_updated DESC ) AS valid_to_raw,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN '9999-12-31'::TIMESTAMP_NTZ
                    ELSE valid_to_raw
              END AS valid_to,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN 'Y'
                    ELSE 'N'
              END AS current_flag,
              'I' AS dml_type
       FROM   (
              SELECT strm.monitor_control_id,
                     strm.monitor_group_id,
                     strm.monitor_control_name,
                     strm.last_updated
              FROM   strm_monitor_control     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'FALSE'
              )	
       )
UNION ALL
SELECT monitor_control_id,
       monitor_group_id,
       monitor_control_name,
       valid_from,
       valid_to,
       current_flag,
       dml_type
FROM   (
       SELECT monitor_control_id,
              monitor_group_id,
              monitor_control_name,
              valid_from,
              LAG ( valid_from ) OVER ( PARTITION BY monitor_control_id ORDER BY valid_from DESC ) AS valid_to_raw,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN '9999-12-31'::TIMESTAMP_NTZ
                    ELSE valid_to_raw
              END AS valid_to,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN 'Y'
                    ELSE 'N'
              END AS current_flag,
              dml_type
       FROM   (
              SELECT strm.monitor_control_id,
                     strm.monitor_group_id,
                     strm.monitor_control_name,
                     strm.last_updated AS valid_from,
                     'I' AS dml_type
              FROM   strm_monitor_control     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'TRUE'
              UNION ALL
              SELECT tgt.monitor_control_id,
                     tgt.monitor_group_id,
                     tgt.monitor_control_name,
                     tgt.valid_from,
                     'D' AS dml_type
              FROM   hist_monitor_control tgt
              WHERE  tgt.monitor_control_id IN
                     (
                     SELECT DISTINCT strm.monitor_control_id
                     FROM   strm_monitor_control     strm
                     WHERE  strm.metadata$action   = 'INSERT'
                     AND    strm.metadata$isupdate = 'TRUE'
                     )
              AND    tgt.current_flag = 'Y'
              )	
       )
UNION ALL
SELECT strm.monitor_control_id,
       strm.monitor_group_id,
       strm.monitor_control_name,
       tgt.valid_from,
       current_timestamp()::TIMESTAMP_NTZ AS valid_to,
       NULL,
       'D' AS dml_type
FROM   hist_monitor_control tgt
INNER JOIN strm_monitor_control strm
   ON  tgt.monitor_control_id   = strm.monitor_control_id
WHERE  strm.metadata$action   = 'DELETE'
AND    strm.metadata$isupdate = 'FALSE'
AND    tgt.current_flag       = 'Y';

/*******************************/
/**** Create Monitor Groups ****/
/*******************************/
INSERT INTO monitor_control ( monitor_control_id, monitor_group_id, monitor_control_name ) SELECT monitor_control_id_seq.NEXTVAL, monitor_group_id, 'SNOWFLAKE_NIST_AC1' FROM monitor_group WHERE monitor_group_name = 'ACCOUNT PARAMETER';
INSERT INTO monitor_control ( monitor_control_id, monitor_group_id, monitor_control_name ) SELECT monitor_control_id_seq.NEXTVAL, monitor_group_id, 'SNOWFLAKE_NIST_AC2' FROM monitor_group WHERE monitor_group_name = 'ACCOUNT PARAMETER';
INSERT INTO monitor_control ( monitor_control_id, monitor_group_id, monitor_control_name ) SELECT monitor_control_id_seq.NEXTVAL, monitor_group_id, 'SNOWFLAKE_NIST_AC3' FROM monitor_group WHERE monitor_group_name = 'ACCOUNT PARAMETER';
INSERT INTO monitor_control ( monitor_control_id, monitor_group_id, monitor_control_name ) SELECT monitor_control_id_seq.NEXTVAL, monitor_group_id, 'SNOWFLAKE_NIST_NP1' FROM monitor_group WHERE monitor_group_name = 'NETWORK POLICY';
INSERT INTO monitor_control ( monitor_control_id, monitor_group_id, monitor_control_name ) SELECT monitor_control_id_seq.NEXTVAL, monitor_group_id, 'SNOWFLAKE_NIST_TT1' FROM monitor_group WHERE monitor_group_name = 'TIME TRAVEL';
INSERT INTO monitor_control ( monitor_control_id, monitor_group_id, monitor_control_name ) SELECT monitor_control_id_seq.NEXTVAL, monitor_group_id, 'SNOWFLAKE_NIST_RE1' FROM monitor_group WHERE monitor_group_name = 'REST_EVENT_HISTORY';

/**************************************/
/**** Historize Monitor Group Data ****/
/**************************************/
MERGE INTO hist_monitor_control tgt
USING v_monitor_control strm
ON    tgt.monitor_control_id   = strm.monitor_control_id
AND   tgt.monitor_group_id     = strm.monitor_group_id
AND   tgt.monitor_control_name = strm.monitor_control_name
AND   tgt.valid_from           = strm.valid_from
WHEN MATCHED AND strm.dml_type = 'D' THEN
UPDATE
SET   tgt.valid_to     = strm.valid_to,
      tgt.current_flag = 'N'
WHEN NOT MATCHED AND strm.dml_type = 'I' THEN
INSERT
(
tgt.monitor_control_id,
tgt.monitor_group_id,
tgt.monitor_control_name,
tgt.valid_from,
tgt.valid_to,
tgt.current_flag
)
VALUES
(
strm.monitor_control_id,
strm.monitor_group_id,
strm.monitor_control_name,
strm.valid_from,
strm.valid_to,
strm.current_flag
);


-- Create Sequence
CREATE OR REPLACE SEQUENCE monitor_parameter_id_seq START WITH 10000;

-- Create tables
CREATE OR REPLACE TABLE monitor_parameter
(
monitor_parameter_id      NUMBER(10) PRIMARY KEY NOT NULL,
monitor_control_id        NUMBER(10)             NOT NULL REFERENCES monitor_control (monitor_control_id ),
monitor_parameter_name    VARCHAR(255)           NOT NULL,
monitor_parameter_value   VARCHAR(255)           NOT NULL,
last_updated            TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL,
CONSTRAINT monitor_parameter_u1 UNIQUE ( monitor_parameter_name )
);

CREATE OR REPLACE TABLE hist_monitor_parameter
(
monitor_parameter_id      NUMBER(10)             NOT NULL,
monitor_control_id        NUMBER(10) PRIMARY KEY NOT NULL,
monitor_parameter_name    VARCHAR(255)           NOT NULL,
monitor_parameter_value   VARCHAR(255)           NOT NULL,
valid_from                TIMESTAMP_NTZ,
valid_to                  TIMESTAMP_NTZ,
current_flag              VARCHAR(1)
);

-- Create Stream
CREATE OR REPLACE STREAM strm_monitor_parameter ON TABLE monitor_parameter;

-- Create historization View
CREATE OR REPLACE VIEW v_monitor_parameter
AS
SELECT monitor_parameter_id,
       monitor_control_id,
       monitor_parameter_name,
       monitor_parameter_value,
       valid_from,
       valid_to,
       current_flag,
       'I' AS dml_type
FROM   (
       SELECT monitor_parameter_id,
              monitor_control_id,
              monitor_parameter_name,
              monitor_parameter_value,
              last_updated        AS valid_from,
              LAG ( last_updated ) OVER ( PARTITION BY monitor_parameter_id ORDER BY last_updated DESC ) AS valid_to_raw,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN '9999-12-31'::TIMESTAMP_NTZ
                    ELSE valid_to_raw
              END AS valid_to,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN 'Y'
                    ELSE 'N'
              END AS current_flag,
              'I' AS dml_type
       FROM   (
              SELECT strm.monitor_parameter_id,
                     strm.monitor_control_id,
                     strm.monitor_parameter_name,
                     strm.monitor_parameter_value,
                     strm.last_updated
              FROM   strm_monitor_parameter     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'FALSE'
              )	
       )
UNION ALL
SELECT monitor_parameter_id,
       monitor_control_id,
       monitor_parameter_name,
       monitor_parameter_value,
       valid_from,
       valid_to,
       current_flag,
       dml_type
FROM   (
       SELECT monitor_parameter_id,
              monitor_control_id,
              monitor_parameter_name,
              monitor_parameter_value,
              valid_from,
              LAG ( valid_from ) OVER ( PARTITION BY monitor_parameter_id ORDER BY valid_from DESC ) AS valid_to_raw,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN '9999-12-31'::TIMESTAMP_NTZ
                    ELSE valid_to_raw
              END AS valid_to,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN 'Y'
                    ELSE 'N'
              END AS current_flag,
              dml_type
       FROM   (
              SELECT strm.monitor_parameter_id,
                     strm.monitor_control_id,
                     strm.monitor_parameter_name,
                     strm.monitor_parameter_value,
                     strm.last_updated AS valid_from,
                     'I' AS dml_type
              FROM   strm_monitor_parameter     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'TRUE'
              UNION ALL
              SELECT tgt.monitor_parameter_id,
                     tgt.monitor_control_id,
                     tgt.monitor_parameter_name,
                     tgt.monitor_parameter_value,
                     tgt.valid_from,
                     'D' AS dml_type
              FROM   hist_monitor_parameter tgt
              WHERE  tgt.monitor_parameter_id IN
                     (
                     SELECT DISTINCT strm.monitor_parameter_id
                     FROM   strm_monitor_parameter     strm
                     WHERE  strm.metadata$action   = 'INSERT'
                     AND    strm.metadata$isupdate = 'TRUE'
                     )
              AND    tgt.current_flag = 'Y'
              )	
       )
UNION ALL
SELECT strm.monitor_parameter_id,
       strm.monitor_control_id,
       strm.monitor_parameter_name,
       strm.monitor_parameter_value,
       tgt.valid_from,
       current_timestamp()::TIMESTAMP_NTZ AS valid_to,
       NULL,
       'D' AS dml_type
FROM   hist_monitor_parameter tgt
INNER JOIN strm_monitor_parameter strm
   ON  tgt.monitor_parameter_id   = strm.monitor_parameter_id
WHERE  strm.metadata$action   = 'DELETE'
AND    strm.metadata$isupdate = 'FALSE'
AND    tgt.current_flag       = 'Y';

/***********************************/
/**** Create Monitor Parameters ****/
/***********************************/
INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'PREVENT_UNLOAD_TO_INLINE_URL', 'TRUE' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_AC2';
INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'REQUIRE_STORAGE_INTEGRATION_FOR_STAGE_CREATION', 'TRUE' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_AC3';
INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'REQUIRE_STORAGE_INTEGRATION_FOR_STAGE_OPERATION', 'TRUE' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_AC3';

/******************************************/
/**** Historize Monitor Parameter Data ****/
/******************************************/
MERGE INTO hist_monitor_parameter tgt
USING v_monitor_parameter strm
ON    tgt.monitor_parameter_id    = strm.monitor_parameter_id
AND   tgt.monitor_control_id      = strm.monitor_control_id
AND   tgt.monitor_parameter_name  = strm.monitor_parameter_name
AND   tgt.monitor_parameter_value = strm.monitor_parameter_value
AND   tgt.valid_from              = strm.valid_from
WHEN MATCHED AND strm.dml_type = 'D' THEN
UPDATE
SET   tgt.valid_to     = strm.valid_to,
      tgt.current_flag = 'N'
WHEN NOT MATCHED AND strm.dml_type = 'I' THEN
INSERT
(
tgt.monitor_parameter_id,
tgt.monitor_control_id,
tgt.monitor_parameter_name,
tgt.monitor_parameter_value,
tgt.valid_from,
tgt.valid_to,
tgt.current_flag
)
VALUES
(
strm.monitor_parameter_id,
strm.monitor_control_id,
strm.monitor_parameter_name,
strm.monitor_parameter_value,
strm.valid_from,
strm.valid_to,
strm.current_flag
);

/****************************/
/**** Create Secure View ****/
/****************************/
CREATE OR REPLACE SECURE VIEW v_monitor_data COPY GRANTS
AS
SELECT mg.monitor_group_id,
       mg.monitor_group_name,
       mc.monitor_control_id,
       mc.monitor_control_name,
       mp.monitor_parameter_id,
       mp.monitor_parameter_name,
       mp.monitor_parameter_value
FROM   monitor_group          mg,
       monitor_control        mc,
       monitor_parameter      mp
WHERE  mg.monitor_group_id    = mc.monitor_group_id
AND    mc.monitor_control_id  = mp.monitor_control_id
ORDER BY mg.monitor_group_name,
         mc.monitor_control_name,
         mp.monitor_parameter_name;

SELECT * FROM v_monitor_data;

GRANT SELECT, REFERENCES ON v_monitor_data TO ROLE IDENTIFIER ( $monitor_owner_role );


/*******************************/
/**** Logging Monitored Output */
/*******************************/

/******************************/
/**** Change to Owner Role ****/
/******************************/
USE ROLE      IDENTIFIER ( $monitor_owner_role   );
USE DATABASE  IDENTIFIER ( $monitor_database     );
USE SCHEMA    IDENTIFIER ( $monitor_owner_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse    );

-- Create Sequence
CREATE OR REPLACE SEQUENCE monitor_log_id_seq START WITH 10000;

-- Create Logging Table
CREATE OR REPLACE TABLE monitor_log
(
monitor_log_id          NUMBER     PRIMARY KEY NOT NULL,
event_description       STRING     NOT NULL,
event_result            STRING     NOT NULL,
monitor_control_name    STRING     NOT NULL,
monitor_parameter_name  STRING     NOT NULL,
monitor_parameter_value STRING     NOT NULL,
last_updated            TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL
);

/***********************************************/
/**** Pattern 1 - Account Parameter Monitoring */
/***********************************************/

/******************************/
/**** Change to Owner Role ****/
/******************************/
USE ROLE      IDENTIFIER ( $monitor_owner_role   );
USE DATABASE  IDENTIFIER ( $monitor_database     );
USE SCHEMA    IDENTIFIER ( $monitor_owner_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse    );

SHOW PARAMETERS LIKE 'prevent_unload_to_inline_url' IN ACCOUNT;

SELECT "value" FROM TABLE ( RESULT_SCAN ( last_query_id()));

CREATE OR REPLACE PROCEDURE sp_get_parameter_value ( P_PARAMETER STRING ) RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt  = "SHOW PARAMETERS LIKE '" + P_PARAMETER + "'IN ACCOUNT";
   var show_stmt = snowflake.createStatement ({ sqlText:sql_stmt });

   show_res = show_stmt.execute();
   show_op  = show_res.next();

   var sql_stmt  = `SELECT "value" FROM TABLE ( RESULT_SCAN ( last_query_id()));`;
   var exec_stmt = snowflake.createStatement ({ sqlText:sql_stmt });

   rec_set = exec_stmt.execute();
   rec_op  = rec_set.next();

   return rec_set.getColumnValue(1);
$$;

CALL sp_get_parameter_value ( 'prevent_unload_to_inline_url' );

CREATE OR REPLACE PROCEDURE sp_check_parameters() RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt  = "";
   var stmt      = "";
   var recset    = "";
   var result    = "";

   var monitor_control_name    = "";
   var monitor_parameter_name  = "";
   var monitor_parameter_value = "";
   var parameter_value         = "";

   sql_stmt += "SELECT monitor_control_name,\n"
   sql_stmt += "       UPPER ( monitor_parameter_name  ),\n"
   sql_stmt += "       UPPER ( monitor_parameter_value )\n"
   sql_stmt += "FROM   MONITOR.reference_owner.v_monitor_data\n"
   sql_stmt += "WHERE  monitor_group_name = 'ACCOUNT PARAMETER';";

   stmt = snowflake.createStatement ({ sqlText:sql_stmt });

   try
   {
       recset = stmt.execute();
       while(recset.next())
       {
           monitor_control_name    = recset.getColumnValue(1);
           monitor_parameter_name  = recset.getColumnValue(2);
           monitor_parameter_value = recset.getColumnValue(3);

           stmt = snowflake.createStatement ( { sqlText: "CALL sp_get_parameter_value(?);",
                                                binds:[monitor_parameter_name] } );

           res = stmt.execute();
           res.next();

           parameter_value = res.getColumnValue(1);

           sql_stmt  = "INSERT INTO monitor_log\n"
           sql_stmt += "SELECT monitor_log_id_seq.NEXTVAL,\n"
           sql_stmt += "       '" + monitor_parameter_name  + "',\n"
           sql_stmt += "       '" + parameter_value         + "',\n"
           sql_stmt += "       '" + monitor_control_name    + "',\n"
           sql_stmt += "       '" + monitor_parameter_name  + "',\n"
           sql_stmt += "       '" + monitor_parameter_value + "',\n"
           sql_stmt += "       current_timestamp()::TIMESTAMP_NTZ\n"
           sql_stmt += "FROM   dual\n"
           sql_stmt += "WHERE  UPPER ( '" + parameter_value + "' ) <> UPPER ( '" + monitor_parameter_value + "' );";

           stmt = snowflake.createStatement ({ sqlText:sql_stmt });

           try
           {
              stmt.execute();
              result = "Success";
           }
           catch { result = sql_stmt; }
       }
       result = "Success";
   }
   catch { result = sql_stmt; }
   return result;
$$;

CALL sp_check_parameters();

SELECT * FROM monitor_log;

USE ROLE accountadmin;
ALTER ACCOUNT SET prevent_unload_to_inline_url = FALSE;

USE ROLE      IDENTIFIER ( $monitor_owner_role   );
USE DATABASE  IDENTIFIER ( $monitor_database     );
USE SCHEMA    IDENTIFIER ( $monitor_owner_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse    );

CALL sp_check_parameters();

SELECT * FROM monitor_log;

USE ROLE      IDENTIFIER ( $monitor_reference_role   );
USE DATABASE  IDENTIFIER ( $monitor_database         );
USE SCHEMA    IDENTIFIER ( $monitor_reference_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse        );

/***************************************/
/**** Setup Reference Owner Objects ****/
/***************************************/
USE ROLE      IDENTIFIER ( $monitor_reference_role   );
USE DATABASE  IDENTIFIER ( $monitor_database         );
USE SCHEMA    IDENTIFIER ( $monitor_reference_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse        );

INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'MY_NETWORK_POLICY', '192.168.0.0' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_NP1';

-- Call MERGE to historise data

USE ROLE accountadmin;

CREATE OR REPLACE NETWORK POLICY my_network_policy
ALLOWED_IP_LIST=( '192.168.0.0' );

SHOW NETWORK POLICIES IN ACCOUNT;

SELECT "name" FROM TABLE ( RESULT_SCAN ( last_query_id()));

DESCRIBE NETWORK POLICY my_network_policy;

SELECT "name", "value" FROM TABLE ( RESULT_SCAN ( last_query_id())); 

/***************************************/
/**** Setup Reference Owner Objects ****/
/***************************************/
USE ROLE      IDENTIFIER ( $monitor_reference_role   );
USE DATABASE  IDENTIFIER ( $monitor_database         );
USE SCHEMA    IDENTIFIER ( $monitor_reference_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse        );

INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'MONITOR', '90' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_TT1';
INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'TEST', '90' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_TT1';

-- Call MERGE to historise data

USE ROLE      IDENTIFIER ( $monitor_owner_role   );
USE DATABASE  IDENTIFIER ( $monitor_database     );
USE SCHEMA    IDENTIFIER ( $monitor_owner_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse    );

INSERT INTO monitor_log
SELECT monitor_log_id_seq.NEXTVAL,
       d.database_name,
       d.retention_time,
       v.monitor_control_name,
       v.monitor_parameter_name,
       v.monitor_parameter_value,
       current_timestamp()::TIMESTAMP_NTZ
FROM   MONITOR.reference_owner.v_monitor_data v,
       snowflake.account_usage.databases      d
WHERE  v.monitor_parameter_name               = d.database_name
AND    v.monitor_parameter_value             != d.retention_time
AND    v.monitor_group_name                   = 'TIME TRAVEL'
AND    d.deleted IS NULL;

/***************************************/
/**** Setup Reference Owner Objects ****/
/***************************************/
USE ROLE      IDENTIFIER ( $monitor_reference_role   );
USE DATABASE  IDENTIFIER ( $monitor_database         );
USE SCHEMA    IDENTIFIER ( $monitor_reference_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse        );

INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'TEST', '90' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_RE1';

-- Call MERGE to historise data

USE ROLE      IDENTIFIER ( $monitor_owner_role   );
USE DATABASE  IDENTIFIER ( $monitor_database     );
USE SCHEMA    IDENTIFIER ( $monitor_owner_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse    );

-- Create Sequence
CREATE OR REPLACE SEQUENCE hist_rest_event_history_id_seq START WITH 10000;

CREATE OR REPLACE TABLE hist_rest_event_history
(
hist_rest_event_history_id    NUMBER,         -- Surrogate key
event_timestamp               TIMESTAMP_LTZ,  -- Time of the event occurrence.
event_id                      NUMBER,         -- The unique identifier for the request.
event_type                    TEXT,           -- The REST API event category. Currently, SCIM is the only possible value.
endpoint                      TEXT,           -- The endpoint in the API request (e.g. scim/v2/Users/<id>).
method                        TEXT,           -- The HTTP method used in the request.
status                        TEXT,           -- The HTTP status result of the request.
error_code                    TEXT,           -- Error code, if the request was not successful.
details                       TEXT,           -- A description of the result of the API request in JSON format.
client_ip                     TEXT,           -- The IP address where the request originated from.
actor_name                    TEXT,           -- The name of the actor making the request.
actor_domain                  TEXT,           -- The domain (i.e. security integration) in which the request was made.
resource_name                 TEXT,           -- The name of the object making the request.
resource_domain               TEXT            -- The object type (e.g. user) making the request.  
);

USE ROLE accountadmin;

CREATE OR REPLACE SECURE VIEW v_rest_event_history COPY GRANTS
AS
SELECT reh.event_timestamp,
       reh.event_id,
       reh.event_type,
       reh.endpoint,
       reh.method,
       reh.status,
       reh.error_code,
       reh.details,
       reh.client_ip,
       reh.actor_name,
       reh.actor_domain,
       reh.resource_name,
       reh.resource_domain,
       current_timestamp()::TIMESTAMP_NTZ  current_timestamp
FROM   TABLE ( snowflake.information_schema.rest_event_history (
                         'scim',
                         DATEADD ( 'minutes', -60, current_timestamp()),
                         current_timestamp(),
                         1000 )
              ) reh;


/***************************************/
/**** Setup Reference Owner Objects ****/
/***************************************/
USE ROLE      IDENTIFIER ( $monitor_reference_role   );
USE DATABASE  IDENTIFIER ( $monitor_database         );
USE SCHEMA    IDENTIFIER ( $monitor_reference_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse        );

INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'ACCOUNTADMIN', '-60' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_AC1';

-- Call MERGE to historise data

/*************************************/
/**** Setup Monitor Owner Objects ****/
/*************************************/
USE ROLE      IDENTIFIER ( $monitor_owner_role   );
USE DATABASE  IDENTIFIER ( $monitor_database     );
USE SCHEMA    IDENTIFIER ( $monitor_owner_schema );
USE WAREHOUSE IDENTIFIER ( $monitor_warehouse    );

CREATE OR REPLACE SECURE VIEW v_snowflake_nist_ac1 COPY GRANTS
COMMENT = 'Reference: Chapter 4 Account Security and our first control'
AS
SELECT 'SNOWFLAKE_NIST_AC1'                          AS control_name,
       'MONITOR.monitor_owner.v_snowflake_nist_ac1'  AS control_object,
       start_time,
       role_name,
       database_name,
       schema_name,
       user_name,
       query_text,
       query_id
FROM   snowflake.account_usage.query_history
WHERE  role_name = 'ACCOUNTADMIN';

SELECT * FROM v_snowflake_nist_ac1;

CREATE OR REPLACE SECURE VIEW v_rt_snowflake_nist_ac1 COPY GRANTS
COMMENT = 'Reference: Chapter 4 Account Security and our first control'
AS
SELECT 'SNOWFLAKE_NIST_AC1'                             AS control_name,
       'MONITOR.monitor_owner.v_rt_snowflake_nist_ac1'  AS control_object,
       start_time,
       role_name,
       database_name,
       schema_name,
       user_name,
       query_text,
       query_id
FROM   TABLE ( snowflake.information_schema.query_history ( DATEADD ( 'days', -1, current_timestamp()), current_timestamp()))
WHERE  role_name = 'ACCOUNTADMIN';

SELECT * FROM v_rt_snowflake_nist_ac1;

CREATE OR REPLACE SECURE VIEW v_rt_snowflake_controls COPY GRANTS
AS
SELECT ac1.control_name,
       ac1.control_object,
       ac1.start_time,
       ac1.role_name,
       ac1.database_name,
       ac1.schema_name,
       ac1.user_name,
       ac1.query_text,
       ac1.query_id,
       rd.monitor_group_name,
       rd.monitor_control_name,
       rd.monitor_parameter_name,
       rd.monitor_parameter_value
FROM   MONITOR.monitor_owner.v_rt_snowflake_nist_ac1   ac1,
       MONITOR.reference_owner.v_monitor_data          rd
WHERE  ac1.control_name          = rd.monitor_control_name
AND    rd.monitor_control_name   = 'SNOWFLAKE_NIST_AC1'
AND    rd.monitor_parameter_name = 'ACCOUNTADMIN'
AND    ac1.start_time           >= DATEADD ( 'minutes', rd.monitor_parameter_value, current_timestamp()::TIMESTAMP_NTZ );

SELECT * FROM v_rt_snowflake_controls;

CREATE OR REPLACE SECURE VIEW login_history COPY GRANTS
AS
SELECT lh.*,
       current_account()||'.'||user_name        AS object_name,
       'Login History Wrapper View'             AS source_name,
       'snowflake.account_usage.login_history'  AS source_path
FROM   snowflake.account_usage.login_history lh;

SELECT * FROM login_history;

GRANT SELECT ON login_history TO ROLE IDENTIFIER ( $monitor_reader_role );

/**********************/
/**** Test Scripts ****/
/**********************
SELECT * FROM hist_monitor_group;

SELECT * FROM v_monitor_group;

UPDATE monitor_group SET monitor_group_name = 'ACCOUNT_PARAMETER' WHERE monitor_group_name = 'ACCOUNT PARAMETER';

INSERT INTO monitor_group ( monitor_group_id, monitor_group_name ) VALUES ( monitor_group_id_seq.NEXTVAL, 'ABC' );

DELETE FROM monitor_group WHERE monitor_group_name = 'ABC';

TRUNCATE TABLE monitor_group;

TRUNCATE TABLE hist_monitor_group;

SELECT tgt.monitor_group_id,
       strm.monitor_group_id,
       tgt.monitor_group_name,
       strm.monitor_group_name,
       tgt.valid_from,
       strm.valid_from,
       tgt.valid_to,
       strm.valid_to,
       strm.dml_type
FROM       v_monitor_group      strm
INNER JOIN hist_monitor_group   tgt       
ON     tgt.monitor_group_id = strm.monitor_group_id
AND    tgt.valid_from       = strm.valid_from;

SELECT tgt.monitor_group_id,
       strm.monitor_group_id,
       tgt.monitor_group_name,
       strm.monitor_group_name,
       tgt.valid_from,
       strm.valid_from,
       tgt.valid_to,
       strm.valid_to,
       strm.dml_type
FROM            v_monitor_group      strm
FULL OUTER JOIN hist_monitor_group   tgt       
ON     tgt.monitor_group_id = strm.monitor_group_id
AND    tgt.valid_from       = strm.valid_from;

SELECT monitor_group_id,
       monitor_group_name,
       valid_from,
       valid_to,
       current_flag,
       'I' AS dml_type
FROM   (
       SELECT monitor_group_id,
              monitor_group_name,
              last_updated        AS valid_from,
              LAG ( last_updated ) OVER ( PARTITION BY monitor_group_id ORDER BY last_updated DESC ) AS valid_to_raw,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN '9999-12-31'::TIMESTAMP_NTZ
                    ELSE valid_to_raw
              END AS valid_to,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN 'Y'
                    ELSE 'N'
              END AS current_flag,
              'I' AS dml_type
       FROM   (
              SELECT strm.monitor_group_id,
                     strm.monitor_group_name,
                     strm.last_updated
              FROM   strm_monitor_group     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'FALSE'
              )	
       );

SELECT monitor_group_id,
       monitor_group_name,
       valid_from,
       valid_to,
       current_flag,
       dml_type
FROM   (
       SELECT monitor_group_id,
              monitor_group_name,
              valid_from,
              LAG ( valid_from ) OVER ( PARTITION BY monitor_group_id ORDER BY valid_from DESC ) AS valid_to_raw,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN '9999-12-31'::TIMESTAMP_NTZ
                    ELSE valid_to_raw
              END AS valid_to,
              CASE
                 WHEN valid_to_raw IS NULL
                    THEN 'Y'
                    ELSE 'N'
              END AS current_flag,
              dml_type
       FROM   (
              SELECT NULL   AS monitor_group_id,
                     strm.monitor_group_name,
                     strm.last_updated AS valid_from,
                     'I'    AS dml_type
              FROM   strm_monitor_group     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'TRUE'
              UNION ALL
              SELECT tgt.monitor_group_id,
                     tgt.monitor_group_name,
                     tgt.valid_from,
                     'U' AS dml_type
              FROM   hist_monitor_group tgt
              WHERE  tgt.monitor_group_id IN
                     (
                     SELECT DISTINCT strm.monitor_group_id
                     FROM   strm_monitor_group     strm
                     WHERE  strm.metadata$action   = 'INSERT'
                     AND    strm.metadata$isupdate = 'TRUE'
                     )
              AND    tgt.current_flag = 'Y'
              )	
       );

              SELECT strm.monitor_group_id,
                     strm.monitor_group_name,
                     strm.last_updated AS valid_from,
                     'I' AS dml_type
              FROM   strm_monitor_group     strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'TRUE';

              SELECT tgt.monitor_group_id,
                     tgt.monitor_group_name,
                     tgt.valid_from,
                     'D' AS dml_type
              FROM   hist_monitor_group tgt
              WHERE  tgt.monitor_group_id IN
                     (
                     SELECT DISTINCT strm.monitor_group_id
                     FROM   strm_monitor_group     strm
                     WHERE  strm.metadata$action   = 'INSERT'
                     AND    strm.metadata$isupdate = 'TRUE'
                     )
              AND    tgt.current_flag = 'Y';

SELECT strm.monitor_group_id,
       strm.monitor_group_name,
       tgt.valid_from,
       current_timestamp()::TIMESTAMP_NTZ AS valid_to,
       NULL,
       'D' AS dml_type
FROM   hist_monitor_group tgt
INNER JOIN strm_monitor_group strm
   ON  tgt.monitor_group_id   = strm.monitor_group_id
WHERE  strm.metadata$action   = 'DELETE'
AND    strm.metadata$isupdate = 'FALSE'
AND    tgt.current_flag       = 'Y';

SELECT mg.monitor_group_id,
       mg.monitor_group_name,
       mc.monitor_control_id,
       mc.monitor_control_name
FROM   monitor_group          mg,
       monitor_control        mc
WHERE  mg.monitor_group_id    = mc.monitor_group_id;

SELECT mg.monitor_group_id,
       mg.monitor_group_name
FROM   monitor_group          mg;
********************/


/********************/
/**** Future Use ****
INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'ACCOUNTADMIN', '-60' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_AC1';
INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'MY_NETWORK_POLICY', '192.168.0.10' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_NP1';
INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'MONITOR', '90' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_TT1';
INSERT INTO monitor_parameter ( monitor_parameter_id, monitor_control_id, monitor_parameter_name, monitor_parameter_value ) SELECT monitor_parameter_id_seq.NEXTVAL, monitor_control_id, 'TEST', '90' FROM monitor_control WHERE monitor_control_name = 'SNOWFLAKE_NIST_TT1';
********************/

/**************************/
/**** Remove Test Case ****/
/**************************
USE ROLE sysadmin;

DROP DATABASE IDENTIFIER ( $monitor_database );

DROP WAREHOUSE IDENTIFIER ( $monitor_warehouse );
 
USE ROLE securityadmin;
 
DROP ROLE IDENTIFIER ( $monitor_reference_role  );
DROP ROLE IDENTIFIER ( $monitor_reader_role );
DROP ROLE IDENTIFIER ( $monitor_owner_role );

USE ROLE useradmin;

DROP USER monitor;
**************************/

