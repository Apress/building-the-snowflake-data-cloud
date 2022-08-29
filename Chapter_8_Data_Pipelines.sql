/******************************************************************
 *
 * File:    Chapter_8_Data_Pipelines.txt
 *
 * Purpose: Sample data pipeline worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

USE ROLE      sysadmin;
USE DATABASE  TEST;
USE DATABASE  COMPUTE_WH;
USE SCHEMA    public;

CREATE OR REPLACE FILE FORMAT TEST.public.test_pipe_format
TYPE                = CSV
FIELD_DELIMITER     = '|'
SKIP_HEADER         = 1
NULL_IF             = ( 'NULL', 'null' )
EMPTY_FIELD_AS_NULL = TRUE
SKIP_BLANK_LINES    = TRUE;

CREATE OR REPLACE STAGE TEST.public.test_stage
STORAGE_INTEGRATION = test_integration
DIRECTORY           = ( ENABLE = TRUE AUTO_REFRESH = TRUE )
ENCRYPTION          = ( TYPE = 'SNOWFLAKE_SSE' )
URL                 = 's3://btsdc-test-bucket/'
FILE_FORMAT         = TEST.public.test_pipe_format;

CREATE OR REPLACE TABLE pipe_load COPY GRANTS
(
id       NUMBER,
content  VARCHAR(255)
);

LIST @TEST.public.test_stage;

CREATE OR REPLACE PIPE test_pipe AS
COPY INTO TEST.public.pipe_load FROM @TEST.public.test_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

SHOW PIPES;

SELECT * FROM pipe_load;

ALTER PIPE test_pipe REFRESH;

SELECT system$pipe_status('TEST.public.test_pipe');

SELECT *
FROM TABLE(validate_pipe_load(PIPE_NAME=>'TEST.public.test_pipe', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

SELECT *
FROM TABLE(information_schema.copy_history(TABLE_NAME=>'PIPE_LOAD', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;

SELECT * FROM pipe_load;

LIST @TEST.public.test_stage;

CREATE OR REPLACE TABLE pipe_load_sqs COPY GRANTS
(
id       NUMBER,
content  VARCHAR(255)
);

CREATE OR REPLACE PIPE test_pipe_sqs
AUTO_INGEST = TRUE
AS
COPY INTO pipe_load_sqs FROM @TEST.public.test_stage
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1);

SHOW PIPES;
SHOW PIPES LIKE 'test_pipe_sqs';

SELECT system$pipe_status('TEST.public.test_pipe_sqs');

SELECT *
FROM TABLE(validate_pipe_load(PIPE_NAME=>'TEST.public.test_pipe_sqs', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())));

SELECT *
FROM TABLE(information_schema.copy_history(TABLE_NAME=>'PIPE_LOAD_SQS', START_TIME=> DATEADD(hours, -1, CURRENT_TIMESTAMP())))
ORDER BY last_load_time DESC;


SELECT * FROM pipe_load_sqs;


/**********************/
/* Temporal Load Data */
/**********************/
CREATE OR REPLACE STAGE TEST.public.test_stage
STORAGE_INTEGRATION = test_integration
DIRECTORY           = ( ENABLE = TRUE AUTO_REFRESH = TRUE )
ENCRYPTION          = ( TYPE = 'SNOWFLAKE_SSE' )
URL                 = 's3://btsdc-test-bucket/'
FILE_FORMAT         = TEST.public.test_pipe_format;

CREATE OR REPLACE STREAM strm_test_stage ON STAGE TEST.public.test_stage;

CREATE OR REPLACE VIEW v_strm_test_stage COPY GRANTS
AS
SELECT '@TEST.public.test_stage/'||relative_path                              AS path_to_file,
       SUBSTR ( relative_path, 1, REGEXP_INSTR ( relative_path, '_20' ) - 1 ) AS table_name,
       size,
       last_modified,
       metadata$action
FROM   strm_test_stage;

/*** Load Files ***/
/*
Create and upload test files
content_test_20220115_130442.txt
id,content,last_updated
1000,ABC,2022-01-15 13:04:42
1001,DEF,2022-01-15 13:04:42
1002,GHI,2022-01-15 13:04:42

content_test_20220115_133505.txt
id,content,last_updated
1000,ABX,2022-01-15 13:35:05
1001,DEF,2022-01-15 13:04:42
1003,JKL,2022-01-15 13:35:05
*/

ALTER STAGE TEST.public.test_stage REFRESH;

list @TEST.public.test_stage;

select $1 from @TEST.public.test_stage/content_test_20220115_130442.txt;

CREATE OR REPLACE TABLE stg_content_test
(
id            NUMBER,
content       VARCHAR(30),
last_updated  TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL
);

CREATE OR REPLACE TABLE scd1_content_test
(
id            NUMBER,
content       VARCHAR(30),
last_updated  TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL
);

CREATE OR REPLACE STREAM strm_scd1_content_test ON TABLE scd1_content_test;

CREATE OR REPLACE TABLE scd2_content_test
(
id            NUMBER,
content       VARCHAR(30),
valid_from    TIMESTAMP_NTZ,
valid_to      TIMESTAMP_NTZ,
current_flag  VARCHAR(1),
decision      VARCHAR(100)
);

CREATE OR REPLACE PROCEDURE sp_stg_to_scd1(P_SOURCE_DATABASE      STRING,
                                           P_SOURCE_TABLE         STRING,
                                           P_SOURCE_ATTRIBUTE     STRING,
                                           P_TARGET_TABLE         STRING,
                                           P_MATCH_ATTRIBUTE      STRING ) RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt     = "";
   var stmt         = "";
   var recset       = "";
   var result       = "";
   var update_cols  = "";
   var debug_string = '';

   sql_stmt  = "INSERT INTO " + P_TARGET_TABLE + "\n"
   sql_stmt += "SELECT *\n";
   sql_stmt += "FROM   " + P_SOURCE_TABLE     + "\n";
   sql_stmt += "WHERE  " + P_SOURCE_ATTRIBUTE + " IN\n";
   sql_stmt += "       (\n";
   sql_stmt += "       SELECT " + P_SOURCE_ATTRIBUTE + "\n";
   sql_stmt += "       FROM   " + P_SOURCE_TABLE     + "\n";
   sql_stmt += "       MINUS\n";
   sql_stmt += "       SELECT " + P_SOURCE_ATTRIBUTE + "\n";
   sql_stmt += "       FROM   " + P_TARGET_TABLE     + "\n";
   sql_stmt += "       );\n\n";

   stmt = snowflake.createStatement ({ sqlText:sql_stmt });
   debug_string += sql_stmt;
   
   try
   {
       recset = stmt.execute();
   }
   catch { result = sql_stmt; }

   sql_stmt  = "SELECT column_name\n"
   sql_stmt += "FROM   " + P_SOURCE_DATABASE + ".information_schema.columns\n"
   sql_stmt += "WHERE  table_name = :1\n"
   sql_stmt += "AND    column_name NOT IN ( :2, :3 )\n"
   sql_stmt += "ORDER BY ordinal_position ASC;\n\n"

   stmt = snowflake.createStatement ({ sqlText:sql_stmt, binds:[ P_TARGET_TABLE, P_SOURCE_ATTRIBUTE, P_MATCH_ATTRIBUTE ] });
   sql_stmt      = sql_stmt.replace(":1", "'" + P_TARGET_TABLE     + "'");
   sql_stmt      = sql_stmt.replace(":2", "'" + P_SOURCE_ATTRIBUTE + "'");
   sql_stmt      = sql_stmt.replace(":3", "'" + P_MATCH_ATTRIBUTE  + "'");
   debug_string += sql_stmt;

   try
   {
       recset = stmt.execute();
       while(recset.next())
       {
          update_cols += "tgt." + recset.getColumnValue(1) + " = stg." + recset.getColumnValue(1) + ",\n       "
       }
       update_cols = update_cols.substring(0, update_cols.length -9)
   }
   catch { result = sql_stmt; }

   sql_stmt  = "UPDATE " + P_TARGET_TABLE + " tgt\n"
   sql_stmt += "SET    " + update_cols    + ",\n";
   sql_stmt += "       tgt." + P_MATCH_ATTRIBUTE + " = stg." + P_MATCH_ATTRIBUTE +"\n";
   sql_stmt += "FROM   " + P_SOURCE_TABLE + " stg\n";
   sql_stmt += "WHERE  tgt." + P_SOURCE_ATTRIBUTE + "  = stg." + P_SOURCE_ATTRIBUTE +"\n";
   sql_stmt += "AND    tgt." + P_MATCH_ATTRIBUTE  + " != stg." + P_MATCH_ATTRIBUTE +";\n\n";

   stmt = snowflake.createStatement ({ sqlText:sql_stmt });
   debug_string += sql_stmt;
   
   try
   {
       recset = stmt.execute();
   }
   catch { result = sql_stmt; }

   sql_stmt  = "DELETE FROM " + P_TARGET_TABLE     + "\n"
   sql_stmt += "WHERE  " + P_SOURCE_ATTRIBUTE + " NOT IN\n";
   sql_stmt += "       (\n";
   sql_stmt += "       SELECT " + P_SOURCE_ATTRIBUTE + "\n";
   sql_stmt += "       FROM   " + P_SOURCE_TABLE     + "\n";
   sql_stmt += "       );\n\n";

   stmt = snowflake.createStatement ({ sqlText:sql_stmt });
   debug_string += sql_stmt;
   
   try
   {
       recset = stmt.execute();
   }
   catch { result = sql_stmt; }

   return debug_string;
//   return result;
$$;

CREATE OR REPLACE VIEW v_content_test
AS
SELECT decision,
       id,
       content,
       valid_from,
       valid_to,
       current_flag,
       'I' AS dml_type
FROM   (
       SELECT 'New Record - Insert or Existing Record - Ignore' AS decision,
              id,
              content,
              last_updated        AS valid_from,
              LAG ( last_updated ) OVER ( PARTITION BY id ORDER BY last_updated DESC ) AS valid_to_raw,
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
              SELECT strm.id,
                     strm.content,
                     strm.last_updated
              FROM   strm_scd1_content_test  strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'FALSE'
              )	
       )
UNION ALL
SELECT decision,
       id,
       content,
       valid_from,
       valid_to,
       current_flag,
       dml_type
FROM   (
       SELECT decision,
              id,
              content,
              valid_from,
              LAG ( valid_from ) OVER ( PARTITION BY id ORDER BY valid_from DESC ) AS valid_to_raw,
              valid_to,
              current_flag,
              dml_type
       FROM   (
              SELECT 'Existing Record - Insert' AS decision,
                     strm.id,
                     strm.content,
                     strm.last_updated            AS valid_from,
                     '9999-12-31'::TIMESTAMP_NTZ  AS valid_to,
                     'Y'                          AS current_flag,
                     'I' AS dml_type
              FROM   strm_scd1_content_test strm
              WHERE  strm.metadata$action   = 'INSERT'
              AND    strm.metadata$isupdate = 'TRUE'
              UNION ALL
              SELECT 'Existing Record - Delete',
                     tgt.id,
                     tgt.content,
                     tgt.valid_from,
                     current_timestamp(),
                     'N',
                     'D' AS dml_type
              FROM   scd2_content_test tgt
              WHERE  tgt.id IN
                     (
                     SELECT DISTINCT strm.id
                     FROM   strm_scd1_content_test strm
                     WHERE  strm.metadata$action   = 'INSERT'
                     AND    strm.metadata$isupdate = 'TRUE'
                     )
              AND    tgt.current_flag = 'Y'
              )	
       )
UNION ALL
SELECT 'Missing Record - Delete',
       strm.id,
       strm.content,
       tgt.valid_from,
       current_timestamp()::TIMESTAMP_NTZ AS valid_to,
       NULL,
       'D' AS dml_type
FROM   scd2_content_test          tgt
INNER JOIN strm_scd1_content_test strm
   ON  tgt.id   = strm.id
WHERE  strm.metadata$action   = 'DELETE'
AND    strm.metadata$isupdate = 'FALSE'
AND    tgt.current_flag       = 'Y';

COPY INTO stg_content_test
FROM @TEST.public.test_stage/content_test_20220115_130442.txt
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PURGE       = TRUE;

CALL sp_stg_to_scd1('TEST', 'STG_CONTENT_TEST', 'ID', 'SCD1_CONTENT_TEST', 'LAST_UPDATED');

TRUNCATE TABLE stg_content_test;

/* Merge the first file into SCD2 table */
MERGE INTO scd2_content_test tgt
USING v_content_test strm
ON    tgt.id         = strm.id
AND   tgt.valid_from = strm.valid_from
AND   tgt.content    = strm.content
WHEN MATCHED AND strm.dml_type = 'U' THEN
UPDATE SET tgt.valid_to        = strm.valid_to,
           tgt.current_flag    = 'N',
           tgt.decision        = strm.decision
WHEN MATCHED AND strm.dml_type = 'D' THEN
UPDATE SET tgt.valid_to        = strm.valid_to,
           tgt.current_flag    = 'N',
           tgt.decision        = strm.decision
WHEN NOT MATCHED AND strm.dml_type = 'I' THEN
INSERT
(
tgt.id,
tgt.content,
tgt.valid_from,
tgt.valid_to,
tgt.current_flag,
tgt.decision
) VALUES (
strm.id,
strm.content,
current_timestamp(),
strm.valid_to,
strm.current_flag,
strm.decision
);

COPY INTO stg_content_test
FROM @TEST.public.test_stage/content_test_20220115_133505.txt
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PURGE       = TRUE;

CALL sp_stg_to_scd1('TEST', 'STG_CONTENT_TEST', 'ID', 'SCD1_CONTENT_TEST', 'LAST_UPDATED');

TRUNCATE TABLE stg_content_test;

SELECT * FROM stg_content_test       ORDER BY id ASC;
SELECT * FROM scd1_content_test      ORDER BY id ASC;
SELECT * FROM strm_scd1_content_test ORDER BY id ASC;

SELECT * FROM v_content_test         ORDER BY id ASC;
SELECT * FROM scd2_content_test      ORDER BY id ASC;

/* Merge the second file into SCD2 table */
MERGE INTO scd2_content_test tgt
USING v_content_test strm
ON    tgt.id         = strm.id
AND   tgt.valid_from = strm.valid_from
AND   tgt.content    = strm.content
WHEN MATCHED AND strm.dml_type = 'U' THEN
UPDATE SET tgt.valid_to        = strm.valid_to,
           tgt.current_flag    = 'N',
           tgt.decision        = strm.decision
WHEN MATCHED AND strm.dml_type = 'D' THEN
UPDATE SET tgt.valid_to        = strm.valid_to,
           tgt.current_flag    = 'N',
           tgt.decision        = strm.decision
WHEN NOT MATCHED AND strm.dml_type = 'I' THEN
INSERT
(
tgt.id,
tgt.content,
tgt.valid_from,
tgt.valid_to,
tgt.current_flag,
tgt.decision
) VALUES (
strm.id,
strm.content,
current_timestamp(),
strm.valid_to,
strm.current_flag,
strm.decision
);


CREATE OR REPLACE PROCEDURE sp_load_test_data() RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt  = "";
   var stmt      = "";
   var recset    = "";
   var result    = "";
   
   var debug_string    = '';

   var path_to_file    = "";
   var table_name      = "";

   sql_stmt  = "SELECT path_to_file,\n"
   sql_stmt += "       table_name\n"
   sql_stmt += "FROM   v_strm_test_stage\n"
   sql_stmt += "WHERE  metadata$action = 'INSERT'\n"
   sql_stmt += "ORDER BY path_to_file ASC;\n\n";

   stmt = snowflake.createStatement ({ sqlText:sql_stmt });

   debug_string = sql_stmt;
   
   try
   {
       recset = stmt.execute();
       while(recset.next())
       {
           path_to_file    = recset.getColumnValue(1);
           table_name      = recset.getColumnValue(2);

           sql_stmt  = "COPY INTO stg_" + table_name + "\n"
           sql_stmt += "FROM " + path_to_file +"\n"
           sql_stmt += "FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)\n"
           sql_stmt += "PURGE       = TRUE;\n\n";

           debug_string = debug_string + sql_stmt;
           
           stmt = snowflake.createStatement ({ sqlText:sql_stmt });

           try
           {
              stmt.execute();
              result = "Success";
           }
           catch { result = sql_stmt; }

           sql_stmt  = "CALL sp_stg_to_scd1('TEST', 'STG_" + table_name + "', 'ID', 'SCD1_" + table_name + "', 'LAST_UPDATED');\n\n";

           debug_string = debug_string + sql_stmt;
           
           stmt = snowflake.createStatement ({ sqlText:sql_stmt });

           try
           {
              stmt.execute();
              result = "Success";
           }
           catch { result = sql_stmt; }

           sql_stmt  = "MERGE INTO scd2_" + table_name + " tgt\n"
           sql_stmt += "USING v_" + table_name + " strm\n"
           sql_stmt += "ON    tgt.id         = strm.id\n"
           sql_stmt += "AND   tgt.valid_from = strm.valid_from\n"
           sql_stmt += "AND   tgt.content    = strm.content\n"
           sql_stmt += "WHEN MATCHED AND strm.dml_type = 'U' THEN\n"
           sql_stmt += "UPDATE SET tgt.valid_to        = strm.valid_to,\n"
           sql_stmt += "           tgt.current_flag    = 'N',\n"
           sql_stmt += "           tgt.decision        = strm.decision\n"
           sql_stmt += "WHEN MATCHED AND strm.dml_type = 'D' THEN\n"
           sql_stmt += "UPDATE SET tgt.valid_to        = strm.valid_to,\n"
           sql_stmt += "           tgt.current_flag    = 'N',\n"
           sql_stmt += "           tgt.decision        = strm.decision\n"
           sql_stmt += "WHEN NOT MATCHED AND strm.dml_type = 'I' THEN\n"
           sql_stmt += "INSERT\n"
           sql_stmt += "(\n"
           sql_stmt += "tgt.id,\n"
           sql_stmt += "tgt.content,\n"
           sql_stmt += "tgt.valid_from,\n"
           sql_stmt += "tgt.valid_to,\n"
           sql_stmt += "tgt.current_flag,\n"
           sql_stmt += "tgt.decision\n"
           sql_stmt += ") VALUES (\n"
           sql_stmt += "strm.id,\n"
           sql_stmt += "strm.content,\n"
           sql_stmt += "current_timestamp(),\n"
           sql_stmt += "strm.valid_to,\n"
           sql_stmt += "strm.current_flag,\n"
           sql_stmt += "strm.decision\n"
           sql_stmt += ");\n\n";

           debug_string = debug_string + sql_stmt;
           
           stmt = snowflake.createStatement ({ sqlText:sql_stmt });

           try
           {
              stmt.execute();
              result = "Success";
           }
           catch { result = sql_stmt; }

           sql_stmt  = "TRUNCATE TABLE stg_" + table_name + ";\n\n";

           debug_string = debug_string + sql_stmt;
           
           stmt = snowflake.createStatement ({ sqlText:sql_stmt });

           try
           {
              stmt.execute();
              result = "Success";
           }
           catch { result = sql_stmt; }
       }
   }
   catch { result = sql_stmt; }
   return debug_string;
//   return result;
$$;

CREATE OR REPLACE VIEW v_strm_test_stage COPY GRANTS
AS
SELECT '@TEST.public.test_stage/'||relative_path                              AS path_to_file,
       SUBSTR ( relative_path, 1, REGEXP_INSTR ( relative_path, '_20' ) - 1 ) AS table_name,
       size,
       last_modified,
       metadata$action
FROM   strm_test_stage;

CALL sp_load_test_data();

SELECT path_to_file,
       table_name
FROM   v_strm_test_stage
WHERE  metadata$action = 'INSERT'
ORDER BY path_to_file ASC;

SELECT * FROM strm_test_stage;

CREATE OR REPLACE TASK task_load_test_data
WAREHOUSE = COMPUTE_WH
SCHEDULE  = '1 minute'
WHEN system$stream_has_data ( 'strm_test_stage' )
AS
CALL sp_load_test_data();

USE ROLE accountadmin;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;

USE ROLE sysadmin;

ALTER TASK task_load_test_data RESUME;

SHOW tasks;

SELECT timestampdiff ( second, current_timestamp, scheduled_time ) as next_run,
       scheduled_time,
       current_timestamp,
       name,
       state
FROM   TABLE ( information_schema.task_history())
WHERE  state = 'SCHEDULED'
ORDER BY completed_time DESC;

ALTER TASK task_load_test_data SUSPEND;

TRUNCATE TABLE scd2_content_test;

SELECT * FROM stg_content_test       ORDER BY id ASC;
SELECT * FROM scd1_content_test      ORDER BY id ASC;
SELECT * FROM strm_scd1_content_test ORDER BY id ASC;

SELECT * FROM v_content_test         ORDER BY id ASC;
SELECT * FROM scd2_content_test      ORDER BY id ASC;


ALTER TASK task_load_test_data SUSPEND;

ALTER STAGE TEST.public.test_stage REFRESH;

LIST @TEST.public.test_stage;

SELECT $1 FROM @TEST.public.test_stage/content_test_20220115_130442.txt;

SELECT generate_column_description ( array_agg ( object_construct(*) ), 'external_table' ) as columns
FROM   TABLE ( infer_schema ( location    => '@TEST.public.test_stage/',
                              file_format => 'TEST.public.test_pipe_format'));

CREATE OR REPLACE EXTERNAL TABLE ext_content_test_20220115_130442
(
id           VARCHAR AS (value:c1::varchar),
content      VARCHAR AS (value:c2::varchar),
last_updated VARCHAR AS (value:c3::varchar)
)
WITH LOCATION = @TEST.public.test_stage/
FILE_FORMAT = (TYPE = CSV SKIP_HEADER = 1)
PATTERN     = content_test_20220115_130442.txt;


ALTER EXTERNAL TABLE ext_content_test_20220115_130442 REFRESH;

SHOW EXTERNAL TABLES;

SELECT $1, metadata$filename FROM @TEST.public.test_stage/;

SELECT * FROM ext_content_test_20220115_130442;




