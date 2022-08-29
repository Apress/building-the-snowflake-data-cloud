/******************************************************************
 *
 * File:    Chapter_9_Data_Presentation.txt
 *
 * Purpose: Sample data ingestion worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

USE ROLE      sysadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
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

CREATE OR REPLACE STREAM strm_test_stage ON STAGE TEST.public.test_stage;

CREATE OR REPLACE VIEW v_strm_test_stage COPY GRANTS
AS
SELECT '@TEST.public.test_stage/'||relative_path                              AS path_to_file,
       SUBSTR ( relative_path, 1, REGEXP_INSTR ( relative_path, '_20' ) - 1 ) AS table_name,
       size,
       last_modified,
       metadata$action
FROM   strm_test_stage;
ALTER STAGE TEST.public.test_stage REFRESH;

LIST @TEST.public.test_stage;

SELECT $1 FROM @TEST.public.test_stage/content_test_20220115_130442.txt;

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

CREATE OR REPLACE TABLE data_quality_exception
(
data_quality_exception_id       NUMBER        NOT NULL,
validation_routine              VARCHAR(255)  NOT NULL,
data_quality_exception_code_id  NUMBER        NOT NULL,
source_object_name              VARCHAR(255)  NOT NULL,
source_attribute_name           VARCHAR(255)  NOT NULL,
source_record_pk_info           VARCHAR(255)  NOT NULL,
insert_timestamp                TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL
);

CREATE OR REPLACE TABLE data_quality_exception_code  
(
data_quality_exception_code_id          NUMBER        NOT NULL,
data_quality_exception_code_name        VARCHAR(255)  NOT NULL,
data_quality_exception_code_description VARCHAR(255)  NOT NULL,
insert_timestamp                        TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL
);

CREATE OR REPLACE SEQUENCE seq_data_quality_exception_id      START WITH 10000;
CREATE OR REPLACE SEQUENCE seq_data_quality_exception_code_id START WITH 10000;

INSERT INTO data_quality_exception_code  VALUES (seq_data_quality_exception_code_id.NEXTVAL, 'ATTRIBUTE_IS_NULL', 'Attribute is declared as NOT NULL but NULL value found', current_timestamp());

CREATE OR REPLACE PROCEDURE sp_is_attribute_null( P_ROUTINE          STRING,
                                                  P_DQ_EXEP_CODE     STRING,
                                                  P_SOURCE_OBJECT    STRING,
                                                  P_SOURCE_ATTRIBUTE STRING,
                                                  P_SOURCE_PK_INFO   STRING ) RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt  = "";
   var stmt      = "";
   var result    = "";

   sql_stmt  = "INSERT INTO data_quality_exception ( data_quality_exception_id, validation_routine, data_quality_exception_code_id, source_object_name, source_attribute_name, source_record_pk_info, insert_timestamp )\n"
   sql_stmt += "SELECT seq_data_quality_exception_id.NEXTVAL,\n"
   sql_stmt += "       :1,\n"
   sql_stmt += "       ( SELECT data_quality_exception_code_id FROM data_quality_exception_code WHERE data_quality_exception_code_name = :2 ),\n"
   sql_stmt += "       :3,\n"
   sql_stmt += "       :4,\n"
   sql_stmt += "       :5,\n"
   sql_stmt += "       current_timestamp()::TIMESTAMP_NTZ\n"
   sql_stmt += "FROM   " + P_SOURCE_OBJECT    + "\n"
   sql_stmt += "WHERE  " + P_SOURCE_ATTRIBUTE + " IS NULL\n"
   sql_stmt += "AND    current_flag = 'Y';\n\n";

   stmt = snowflake.createStatement ({ sqlText:sql_stmt, binds:[P_ROUTINE, P_DQ_EXEP_CODE, P_SOURCE_OBJECT, P_SOURCE_ATTRIBUTE, P_SOURCE_PK_INFO] });
   try
   {
       result = stmt.execute();
       result = "Number of rows found: " + stmt.getNumRowsAffected();
   }
   catch { result = sql_stmt; }
   return result;
$$;

CALL sp_is_attribute_null ( 'Test',
                            'ATTRIBUTE_IS_NULL',
                            'scd2_content_test',
                            'content',
                            'id');

SELECT * FROM data_quality_exception;

TRUNCATE TABLE data_quality_exception;

CREATE OR REPLACE PROCEDURE sp_validate_test_data() RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var stmt      = "";
   var result    = "";

   stmt = snowflake.createStatement ({ sqlText: "CALL sp_is_attribute_null(?,?,?,?,?);",
                                       binds:['Test', 'ATTRIBUTE_IS_NULL', 'scd2_content_test', 'id', 'id'] });

   try
   {
       result = stmt.execute();
       result = "SUCCESS";
   }
   catch { result = sql_stmt; }

   stmt = snowflake.createStatement ({ sqlText: "CALL sp_is_attribute_null(?,?,?,?,?);",
                                       binds:['Test', 'ATTRIBUTE_IS_NULL', 'scd2_content_test', 'content', 'id'] });

   try
   {
       result = stmt.execute();
       result = "SUCCESS";
   }
   catch { result = sql_stmt; }
   return result;
$$;

CALL sp_validate_test_data ();

USE ROLE accountadmin;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;

USE ROLE sysadmin;

CREATE OR REPLACE STREAM strm_scd2_content_test ON TABLE scd2_content_test;

CREATE OR REPLACE TASK task_validate_test_data
WAREHOUSE = COMPUTE_WH
SCHEDULE  = '1 minute'
WHEN system$stream_has_data ( 'strm_scd2_content_test' )
AS
CALL sp_validate_test_data();

ALTER TASK task_validate_test_data RESUME;

SHOW tasks;

SELECT timestampdiff ( second, current_timestamp, scheduled_time ) as next_run,
       scheduled_time,
       current_timestamp,
       name,
       state
FROM   TABLE ( information_schema.task_history())
WHERE  state = 'SCHEDULED'
ORDER BY completed_time DESC;

ALTER TASK task_validate_test_data SUSPEND;


/***********************/
/* Simple Data Masking */
/***********************/
USE ROLE securityadmin;

CREATE OR REPLACE ROLE maskingadmin;

GRANT CREATE MASKING POLICY ON SCHEMA TEST.public TO ROLE maskingadmin;

GRANT USAGE ON DATABASE  TEST        TO ROLE maskingadmin;
GRANT USAGE ON WAREHOUSE compute_wh  TO ROLE maskingadmin;
GRANT USAGE ON SCHEMA    TEST.public TO ROLE maskingadmin;

GRANT ROLE maskingadmin TO USER <Your User Here>;

USE ROLE accountadmin;

GRANT APPLY MASKING POLICY ON ACCOUNT TO ROLE maskingadmin;


USE ROLE sysadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    public;

CREATE OR REPLACE TABLE masking_test
(
user_email        VARCHAR(30)  NOT NULL,
user_email_status VARCHAR(30)  NOT NULL
);

CREATE OR REPLACE STREAM strm_masking_test ON TABLE masking_test;

INSERT INTO masking_test
VALUES
('user_1@masking_test.com', 'Public' ),
('user_2@masking_test.com', 'Private');

SELECT * FROM masking_test;

CREATE OR REPLACE VIEW TEST.public.v_masking_test
AS
SELECT * FROM masking_test;

GRANT SELECT ON masking_test        TO ROLE maskingadmin;
GRANT SELECT ON v_masking_test      TO ROLE maskingadmin;
GRANT SELECT ON strm_masking_test   TO ROLE maskingadmin;

SELECT * FROM strm_masking_test;

SHOW masking policies;
SHOW masking policies IN ACCOUNT;

USE ROLE      maskingadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    TEST.public;

SELECT * FROM masking_test;

USE ROLE accountadmin;

DROP MASKING POLICY IF EXISTS dq_code_mask;

USE ROLE maskingadmin;

CREATE OR REPLACE MASKING POLICY dq_code_mask AS ( P_PARAM STRING ) RETURNS STRING ->
CASE
   WHEN current_role() IN ('SYSADMIN') THEN P_PARAM
   ELSE '*********'
END;

SHOW masking policies lIKE 'dq_code_mask';

ALTER TABLE TEST.public.masking_test
MODIFY COLUMN user_email SET MASKING POLICY dq_code_mask;

SELECT * FROM masking_test;

SELECT * FROM v_masking_test;

SELECT * FROM strm_masking_test;

USE ROLE sysadmin;

SELECT * FROM masking_test;

USE ROLE maskingadmin;

ALTER TABLE TEST.public.masking_test
MODIFY COLUMN user_email UNSET MASKING POLICY;

DROP MASKING POLICY dq_code_mask;


/****************************/
/* Conditional Data Masking */
/****************************/
CREATE OR REPLACE MASKING POLICY dq_code_mask AS ( user_email VARCHAR, user_email_status VARCHAR ) RETURNS STRING ->
CASE
   WHEN current_role() IN ('SYSADMIN') THEN user_email
   WHEN user_email_status = 'Public'   THEN user_email
   ELSE '*********'
END;

ALTER TABLE TEST.public.masking_test
MODIFY COLUMN user_email
SET MASKING POLICY dq_code_mask
USING (user_email, user_email_status);

SELECT * FROM masking_test;

SELECT * FROM v_masking_test;

SELECT * FROM strm_masking_test;

USE ROLE sysadmin;

SELECT * FROM masking_test;

USE ROLE maskingadmin;

ALTER TABLE TEST.public.masking_test
MODIFY COLUMN user_email UNSET MASKING POLICY;

DROP MASKING POLICY dq_code_mask;


/**********************/
/* Row Level Security */
/**********************/

USE ROLE accountadmin;

GRANT APPLY ROW ACCESS POLICY ON ACCOUNT TO ROLE maskingadmin;

USE ROLE sysadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    public;

SELECT * FROM masking_test;

CREATE OR REPLACE ROW ACCESS POLICY sysadmin_policy
AS ( user_email VARCHAR ) RETURNS BOOLEAN ->
'SYSADMIN' = current_role();

ALTER TABLE TEST.public.masking_test
ADD ROW ACCESS POLICY sysadmin_policy ON ( user_email );

SELECT *
FROM   TABLE ( information_schema.policy_references ( policy_name => 'sysadmin_policy' ));

SELECT * FROM masking_test;

USE ROLE      maskingadmin;

SELECT * FROM masking_test;

USE ROLE      sysadmin;

ALTER TABLE TEST.public.masking_test
DROP ROW ACCESS POLICY sysadmin_policy;

DROP ROW ACCESS POLICY sysadmin_policy;


/******************/
/* Object Tagging */
/******************/
USE ROLE securityadmin;

CREATE OR REPLACE ROLE tagadmin;

GRANT USAGE ON DATABASE  TEST        TO ROLE tagadmin;
GRANT USAGE ON WAREHOUSE compute_wh  TO ROLE tagadmin;
GRANT USAGE ON SCHEMA    TEST.public TO ROLE tagadmin;

GRANT CREATE TAG ON SCHEMA TEST.public TO ROLE tagadmin;

GRANT ROLE tagadmin TO USER ANDYC;

USE ROLE accountadmin;

GRANT APPLY TAG ON ACCOUNT TO ROLE tagadmin;

USE ROLE      sysadmin;

CREATE OR REPLACE TABLE pii_test
(
id                     NUMBER,
full_name              VARCHAR(255),
social_security_number VARCHAR(255),
gender                 VARCHAR(255),
date_of_birth          TIMESTAMP_NTZ
);

CREATE OR REPLACE SEQUENCE seq_pii_test_id      START WITH 10000;

INSERT INTO pii_test VALUES (seq_pii_test_id.NEXTVAL, 'John Doe', '12345678', 'Male', current_timestamp()), (seq_pii_test_id.NEXTVAL, 'Jane Doe', '23456789', 'Female', current_timestamp());

GRANT SELECT ON pii_test TO ROLE tagadmin;

USE ROLE      tagadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    public;

CREATE OR REPLACE TAG PII            COMMENT = 'Personally Identifiable Information';
CREATE OR REPLACE TAG PII_S_FullName COMMENT = 'Personally Identifiable Information -> Sensitive -> Full Name';
CREATE OR REPLACE TAG PII_S_SSN      COMMENT = 'Personally Identifiable Information -> Sensitive -> Social Security Number';
CREATE OR REPLACE TAG PII_N_Gender   COMMENT = 'Personally Identifiable Information -> Non-Sensitive -> Gender';
CREATE OR REPLACE TAG PII_N_DoB      COMMENT = 'Personally Identifiable Information -> Non-Sensitive -> Date of Birth';

SHOW tags;

SELECT * FROM pii_test;

ALTER TABLE pii_test SET TAG PII = 'Personally Identifiable Information';

ALTER TABLE pii_test MODIFY COLUMN full_name              SET TAG PII_S_FullName = 'Personally Identifiable Information -> Sensitive -> Full Name';
ALTER TABLE pii_test MODIFY COLUMN social_security_number SET TAG PII_S_SSN      = 'Personally Identifiable Information -> Sensitive -> Social Security Number';
ALTER TABLE pii_test MODIFY COLUMN gender                 SET TAG PII_N_Gender   = 'Personally Identifiable Information -> Non-Sensitive -> Gender';
ALTER TABLE pii_test MODIFY COLUMN date_of_birth          SET TAG PII_N_DoB      = 'Personally Identifiable Information -> Non-Sensitive -> Date of Birth';

SELECT *
FROM   TABLE ( TEST.information_schema.tag_references ( 'pii_test', 'TABLE' ));

SELECT *
FROM   TABLE ( TEST.information_schema.tag_references( 'pii_test.full_name', 'COLUMN' ));

SELECT system$get_tag ( 'PII', 'pii_test', 'TABLE' );
SELECT system$get_tag ( 'PII_S_FullName', 'pii_test.full_name', 'COLUMN' );

SELECT *
FROM   TABLE ( information_schema.tag_references_all_columns ( 'pii_test', 'TABLE' ));

ALTER TAG PII ADD ALLOWED_VALUES 'Personally Identifiable Information', 'PII Admin: pii@your_org.xyz';

SELECT system$get_tag_allowed_values ( 'TEST.public.PII' );

ALTER TAG PII UNSET ALLOWED_VALUES;

ALTER TABLE pii_test MODIFY COLUMN full_name              UNSET TAG PII_S_FullName;
ALTER TABLE pii_test MODIFY COLUMN social_security_number UNSET TAG PII_S_SSN;
ALTER TABLE pii_test MODIFY COLUMN gender                 UNSET TAG PII_N_Gender;
ALTER TABLE pii_test MODIFY COLUMN date_of_birth          UNSET TAG PII_N_DoB;

DROP TAG PII;
DROP TAG PII_S_FullName;
DROP TAG PII_S_SSN;
DROP TAG PII_N_Gender;
DROP TAG PII_N_DoB;



