/******************************************************************
 *
 * File:    Chapter_12_Data_Management.txt
 *
 * Purpose: Sample data management worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

/*****************************/
/* Object Tagging Automation */
/*****************************/
USE ROLE sysadmin;
USE DATABASE  TEST;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    public;

CREATE OR REPLACE FILE FORMAT TEST.public.test_csv_format
TYPE                = CSV
FIELD_DELIMITER     = ','
SKIP_HEADER         = 1
NULL_IF             = ( 'NULL', 'null' )
EMPTY_FIELD_AS_NULL = TRUE
SKIP_BLANK_LINES    = TRUE;

SHOW file formats;

/******************/
/* Data Custodian */
/******************/
CREATE OR REPLACE TABLE constructor
(
dataset               VARCHAR(255),
position              NUMBER,
label                 VARCHAR(255),
source_attribute      VARCHAR(255),
source_datatype       VARCHAR(255),
source_precision      VARCHAR(255),
target_attribute      VARCHAR(255),
target_datatype       VARCHAR(255),
target_precision      VARCHAR(255)
);

CREATE OR REPLACE PROCEDURE sp_construct( P_DATASET         STRING,
                                          P_AUTODEPLOY      STRING ) RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt           = "";
   var stmt               = "";
   var recset             = "";
   var result             = "";
   var stg_table_name     = "";
   var stg_column_name    = "";
   var strm_table_name    = "";
   var seq_stg_table_name = "";
   var stg_pk_column_name = "";
   var stg_at_column_name = "";
   var stg_dt_column_name = "";
   var debug_string       = '';
   sql_stmt  = "SELECT LOWER ( source_attribute ),\n";
   sql_stmt += "       LOWER ( source_datatype  ),\n";
   sql_stmt += "       label\n";
   sql_stmt += "FROM   constructor\n";
   sql_stmt += "WHERE  label IN ( 'STAGING_TABLE' )\n";
   sql_stmt += "AND    dataset = :1;\n\n";
   debug_string += sql_stmt;
   stmt = snowflake.createStatement ({ sqlText:sql_stmt, binds:[P_DATASET] });
   try
   {
      recset = stmt.execute();
      while(recset.next())
      {
	 if( recset.getColumnValue(3) == "STAGING_TABLE")
         {
            stg_table_name     = recset.getColumnValue(2) + "." + recset.getColumnValue(1);
            strm_table_name    = recset.getColumnValue(2) + "." + "strm_" + recset.getColumnValue(1);
            seq_stg_table_name = recset.getColumnValue(2) + "." + "seq_"  + recset.getColumnValue(1) + "_id";
         }
      }
   }
   catch { result = sql_stmt; }
   sql_stmt  = "SELECT LOWER ( source_attribute ),\n";
   sql_stmt += "       LOWER ( source_datatype  ),\n";
   sql_stmt += "       LOWER ( source_precision ),\n";
   sql_stmt += "       LOWER ( target_attribute ),\n";
   sql_stmt += "       LOWER ( target_datatype  ),\n";
   sql_stmt += "       LOWER ( target_precision ),\n";
   sql_stmt += "       label\n";
   sql_stmt += "FROM   constructor\n";
   sql_stmt += "WHERE  dataset = :1\n";
   sql_stmt += "AND    label IN ( 'PRIMARY_KEY', 'ATTRIBUTE', 'DATE_MASTER_KEY' )\n"
   sql_stmt += "ORDER BY TO_NUMBER ( position ) ASC;\n\n";
   debug_string += sql_stmt;
   stmt = snowflake.createStatement ({ sqlText:sql_stmt, binds:[P_DATASET] });
   try
   {
      recset = stmt.execute();
      while(recset.next())
      {
         stg_column_name += recset.getColumnValue(1) + " " + recset.getColumnValue(2) + "(" + recset.getColumnValue(3) + "),\n"

	 if( recset.getColumnValue(7) == "PRIMARY_KEY")
         {
            stg_pk_column_name += recset.getColumnValue(1) + ",\n"
         }
         else if( recset.getColumnValue(7) == "ATTRIBUTE")
         {
            stg_at_column_name += recset.getColumnValue(1) + ",\n"
         }
         else if( recset.getColumnValue(7) == "DATE_MASTER_KEY")
         {
            stg_dt_column_name += recset.getColumnValue(1)
         }
      }
   }
   catch { result = sql_stmt; }
   stg_column_name = stg_column_name.substring(0, stg_column_name.length -2)
   sql_stmt  = "CREATE OR REPLACE SEQUENCE " + seq_stg_table_name + " START = 1 INCREMENT = 1;\n\n";
   debug_string += sql_stmt;
   if( P_AUTODEPLOY == "TRUE" )
   {
      try
      {
         stmt = snowflake.createStatement ({ sqlText:sql_stmt });
         recset = stmt.execute();
      }
      catch { result = sql_stmt; }
   }
   sql_stmt  = "CREATE OR REPLACE TABLE " + stg_table_name + "\n";
   sql_stmt += "(\n"
   sql_stmt += stg_column_name + "\n"
   sql_stmt += ")\n";
   sql_stmt += "COPY GRANTS;\n\n";
   debug_string += sql_stmt;
   if( P_AUTODEPLOY == "TRUE" )
   {
      try
      {
         stmt = snowflake.createStatement ({ sqlText:sql_stmt });
         recset = stmt.execute();
      }
      catch { result = sql_stmt; }
   }
   sql_stmt  = "CREATE OR REPLACE STREAM " + strm_table_name + " ON TABLE " + stg_table_name + ";\n\n";
   debug_string += sql_stmt;
   if( P_AUTODEPLOY == "TRUE" )
   {
      try
      {
         stmt = snowflake.createStatement ({ sqlText:sql_stmt });
         recset = stmt.execute();
      }
      catch { result = sql_stmt; }
   }
   return debug_string;
//   return result;
$$;

CALL sp_construct ( 'EMPLOYEE',
                    'FALSE' );
                    
CALL sp_construct ( 'EMPLOYEE',
                    'TRUE' );

SELECT seq_stg_employee_id.NEXTVAL;

SELECT * FROM strm_stg_employee;


/*****************************/
/* Object Tagging Automation */
/*****************************/
CREATE OR REPLACE TABLE employee
(
employee_id               NUMBER,
preferred_name            VARCHAR(255),
surname_preferred         VARCHAR(255),
forename_preferred        VARCHAR(255),
gender                    VARCHAR(255),
national_insurance_number VARCHAR(255),
social_security_number    VARCHAR(255),
postcode                  VARCHAR(255),
zip_code                  VARCHAR(255),
salary                    NUMBER
);

CREATE OR REPLACE SEQUENCE seq_employee_id START WITH 10000;

CREATE OR REPLACE TABLE tag
(
tag_id  NUMBER PRIMARY KEY NOT NULL,
name    VARCHAR(255)       NOT NULL,
comment VARCHAR(2000)
);

CREATE OR REPLACE SEQUENCE seq_tag_id START WITH 10000;

INSERT INTO tag ( tag_id, name, comment ) VALUES
(seq_tag_id.NEXTVAL, 'PII',          'Personally Identifiable Information'),
(seq_tag_id.NEXTVAL, 'PII_S_Name',   'Personally Identifiable Information -> Sensitive -> Name'),
(seq_tag_id.NEXTVAL, 'PII_N_Gender', 'Personally Identifiable Information -> Non-Sensitive -> Gender');

CREATE OR REPLACE TABLE object
(
object_id       NUMBER PRIMARY KEY NOT NULL,
database        VARCHAR(255)       NOT NULL,
schema          VARCHAR(255)       NOT NULL,
name            VARCHAR(255)       NOT NULL,
attribute       VARCHAR(255)
);

CREATE OR REPLACE SEQUENCE seq_object_id START WITH 10000;

INSERT INTO object ( object_id, database, schema, name, attribute ) VALUES
(seq_object_id.NEXTVAL, 'TEST', 'public', 'employee',  NULL),
(seq_object_id.NEXTVAL, 'TEST', 'public', 'employee',  'preferred_name'),
(seq_object_id.NEXTVAL, 'TEST', 'public', 'employee',  'surname_preferred'),
(seq_object_id.NEXTVAL, 'TEST', 'public', 'employee',  'forename_preferred'),
(seq_object_id.NEXTVAL, 'TEST', 'public', 'employee',  'gender');

CREATE OR REPLACE TABLE tag_object
(
tag_object_id NUMBER    NOT NULL,
tag_id        NUMBER    NOT NULL REFERENCES tag (tag_id),
object_id              NUMBER    NOT NULL REFERENCES object       (object_id) -- Foreign key
);

CREATE OR REPLACE SEQUENCE seq_tag_object_id START WITH 10000;

INSERT INTO tag_object ( tag_object_id, tag_id, object_id )
WITH
c AS (SELECT tag_id FROM tag WHERE name = 'PII'),
o AS (SELECT object_id FROM object WHERE database = 'TEST' AND schema = 'public' AND name = 'employee' AND attribute IS NULL)
SELECT seq_tag_object_id.NEXTVAL, c.tag_id, o.object_id
FROM   c, o;

INSERT INTO tag_object ( tag_object_id, tag_id, object_id )
WITH
c AS (SELECT tag_id FROM tag WHERE name = 'PII_S_Name'),
o AS (SELECT object_id FROM object WHERE database = 'TEST' AND schema = 'public' AND name = 'employee' AND attribute = 'preferred_name')
SELECT seq_tag_object_id.NEXTVAL, c.tag_id, o.object_id
FROM   c, o;

INSERT INTO tag_object ( tag_object_id, tag_id, object_id )
WITH
c AS (SELECT tag_id FROM tag WHERE name = 'PII_S_Name'),
o AS (SELECT object_id FROM object WHERE database = 'TEST' AND schema = 'public' AND name = 'employee' AND attribute = 'surname_preferred')
SELECT seq_tag_object_id.NEXTVAL, c.tag_id, o.object_id
FROM   c, o;

INSERT INTO tag_object ( tag_object_id, tag_id, object_id )
WITH
c AS (SELECT tag_id FROM tag WHERE name = 'PII_S_Name'),
o AS (SELECT object_id FROM object WHERE database = 'TEST' AND schema = 'public' AND name = 'employee' AND attribute = 'forename_preferred')
SELECT seq_tag_object_id.NEXTVAL, c.tag_id, o.object_id
FROM   c, o;

INSERT INTO tag_object ( tag_object_id, tag_id, object_id )
WITH
c AS (SELECT tag_id FROM tag WHERE name = 'PII_N_Gender'),
o AS (SELECT object_id FROM object WHERE database = 'TEST' AND schema = 'public' AND name = 'employee' AND attribute = 'gender')
SELECT seq_tag_object_id.NEXTVAL, c.tag_id, o.object_id
FROM   c, o;

CREATE OR REPLACE VIEW v_tag_object
AS
SELECT cto.tag_object_id,
       ct.tag_id,
       o.object_id,
       ct.name        AS tag_name,
       ct.comment     AS tag_comment,
       o.database,
       o.schema,
       o.name         AS object_name,
       o.attribute
FROM   tag_object     cto,
       tag            ct,
       object         o
WHERE  cto.tag_id     = ct.tag_id
AND    cto.object_id  = o.object_id;

CREATE OR REPLACE VIEW v_tags COPY GRANTS
AS
SELECT 'CREATE OR REPLACE TAG '||tag_name||' COMMENT = '''||tag_comment||''';' AS create_tag_stmt,
       CASE
          WHEN attribute IS NULL THEN
             'ALTER TABLE '||database||'.'||schema||'.'||object_name||' SET TAG '||tag_name||' = '''||tag_comment||''';'
          ELSE
             'ALTER TABLE '||database||'.'||schema||'.'||object_name||' MODIFY COLUMN '||attribute||' SET TAG '||tag_name||' = '''||tag_comment||''';'
       END AS apply_tag_stmt,
       tag_name,
       tag_comment,
       database,
       schema,
       object_name,
       attribute
FROM   v_tag_object;

SELECT * FROM v_tags;
 

CREATE OR REPLACE PROCEDURE sp_apply_object_tag() RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt        = "";
   var stmt            = "";
   var create_tag_stmt = "";
   var apply_tag_stmt  = "";
   var result          = "";
   var retval          = "";
   var debug_string    = "";

   sql_stmt  = "SELECT DISTINCT create_tag_stmt\n"
   sql_stmt += "FROM   v_tags\n"
   sql_stmt += "ORDER BY create_tag_stmt ASC;\n\n"

   stmt = snowflake.createStatement ({ sqlText:sql_stmt });
   debug_string += sql_stmt;
   try
   {
      result = stmt.execute();
      while(result.next())
      {
         create_tag_stmt = result.getColumnValue(1);
         stmt = snowflake.createStatement ({ sqlText:create_tag_stmt });
         debug_string += create_tag_stmt + "\n";

         try
         {
            retval = stmt.execute();
            retval = "Success";
         }
         catch (err) { retval = create_tag_stmt + "\nCode: " + err.code + "\nState: " + err.state + "\nMessage: " + err.message + "\nStack Trace: " + err.stackTraceTxt }
      }
   }
   catch (err) { retval = sql_stmt + "Code: " + err.code + "\nState: " + err.state + "\nMessage: " + err.message + "\nStack Trace: " + err.stackTraceTxt }

   sql_stmt  = "\nSELECT apply_tag_stmt\n"
   sql_stmt += "FROM   v_tags\n"
   sql_stmt += "ORDER BY create_tag_stmt ASC;\n\n"

   stmt = snowflake.createStatement ({ sqlText:sql_stmt });
   debug_string += sql_stmt;
   try
   {
      result = stmt.execute();
      while(result.next())
      {
         apply_tag_stmt = result.getColumnValue(1);
         stmt = snowflake.createStatement ({ sqlText:apply_tag_stmt });
         debug_string += apply_tag_stmt + "\n";

         try
         {
            retval = stmt.execute();
            retval = "Success";
         }
         catch (err) { retval = apply_tag_stmt + "\nCode: " + err.code + "\nState: " + err.state + "\nMessage: " + err.message + "\nStack Trace: " + err.stackTraceTxt }
         retval = "Success";
      }
   }
   catch (err) { retval = sql_stmt + "Code: " + err.code + "\nState: " + err.state + "\nMessage: " + err.message + "\nStack Trace: " + err.stackTraceTxt }
   return debug_string;
//   return retval;
$$;

CALL sp_apply_object_tag();

SELECT system$get_tag('PII', 'employee', 'TABLE');

SELECT system$get_tag('PII_N_GENDER', 'employee.gender', 'COLUMN');

SELECT *
FROM   TABLE ( information_schema.tag_references_all_columns ( 'employee', 'TABLE' ));

SELECT *
FROM   TABLE ( information_schema.tag_references_all_columns ( 'employee', 'TABLE' ))
WHERE  tag_name IN ( 'PII', 'PII_N_GENDER' );


USE ROLE accountadmin;

SELECT * FROM snowflake.account_usage.tags
WHERE  deleted IS NULL
ORDER BY tag_name;

GRANT EXECUTE TASK ON ACCOUNT TO ROLE sysadmin;

USE ROLE sysadmin;

CREATE OR REPLACE STREAM strm_tag_object ON TABLE tag_object;

CREATE OR REPLACE TASK task_apply_object_tag
WAREHOUSE = COMPUTE_WH
SCHEDULE  = '1 minute'
WHEN system$stream_has_data ( 'strm_tag_object' )
AS
CALL sp_apply_object_tag();

ALTER TASK task_apply_object_tag RESUME;

SELECT timestampdiff ( second, current_timestamp, scheduled_time ) as next_run,
       scheduled_time,
       current_timestamp,
       name,
       state
FROM   TABLE ( information_schema.task_history())
WHERE  state = 'SCHEDULED'
ORDER BY completed_time DESC;

ALTER TASK task_apply_object_tag SUSPEND;


/**************************/
/**** Remove Test Case ****/
/**************************
ALTER TABLE employee UNSET TAG PII;
ALTER TABLE employee MODIFY COLUMN preferred_name     UNSET TAG PII_S_Name;
ALTER TABLE employee MODIFY COLUMN surname_preferred  UNSET TAG PII_S_Name;
ALTER TABLE employee MODIFY COLUMN forename_preferred UNSET TAG PII_S_Name;
ALTER TABLE employee MODIFY COLUMN gender             UNSET TAG PII_N_Gender;

DROP TAG   PII;
DROP TAG   PII_S_FullName;
DROP TAG   PII_N_Gender;
DROP VIEW  v_tags;
DROP VIEW  v_tag_object;
DROP TABLE tag_object;
DROP TABLE tag;
DROP TABLE object;
DROP PROCEDURE sp_apply_object_tag();
DROP STREAM    strm_tag_object;
DROP TASK      task_apply_object_tag;
*/


/***********************/
/* Data Classification */
/***********************/
USE ROLE   accountadmin;
USE SCHEMA snowflake.core;

SHOW tags IN ACCOUNT;

SHOW tags IN SCHEMA snowflake.core;

SELECT "name", "comment" FROM TABLE ( RESULT_SCAN ( last_query_id()));

/*
SELECT *
FROM   snowflake.account_usage.tags
WHERE  deleted IS NULL
ORDER BY tag_id;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES;

SELECT * FROM SNOWFLAKE.ACCOUNT_USAGE.TAG_REFERENCES
    WHERE TAG_NAME = 'PRIVACY_CATEGORY'
    AND TAG_VALUE= 'IDENTIFIER';

SELECT system$get_tag_allowed_values ( 'snowflake.core.PRIVACY_CATEGORY' );
SELECT system$get_tag_allowed_values ( 'snowflake.core.SEMANTIC_CATEGORY' );
*/

USE ROLE     sysadmin;
USE DATABASE TEST;

SELECT *
FROM   TABLE ( information_schema.tag_references_all_columns ( 'employee', 'TABLE' ));

SELECT extract_semantic_categories( 'TEST.public.employee' );

INSERT INTO employee ( employee_id, preferred_name, surname_preferred, forename_preferred, gender, national_insurance_number, social_security_number, postcode, zip_code, salary ) VALUES
(seq_employee_id.NEXTVAL, 'John Doe', 'Doe', 'John', 'Male', 'FA123456Z', '123-45-6789', 'DN13 7ZZ', 99950, 100000 ),
(seq_employee_id.NEXTVAL, 'Jane Doe', 'Doe', 'Jane', 'Female', 'XS123456Y', '234-56-7890', 'YP22 9HG', 99949, 120000 );

SELECT * FROM employee;

SELECT extract_semantic_categories( 'TEST.public.employee' );

CREATE OR REPLACE TABLE found_data_classifiers COPY GRANTS
(
path_to_object    VARCHAR(2000) NOT NULL,
attribute         VARCHAR(2000) NOT NULL,
privacy_category  VARCHAR(2000) NOT NULL,
semantic_category VARCHAR(2000) NOT NULL,
probability       NUMBER        NOT NULL,
deploy_flag       VARCHAR(1)    DEFAULT 'N' NOT NULL,
last_updated      TIMESTAMP_NTZ DEFAULT current_timestamp()::TIMESTAMP_NTZ NOT NULL
);

INSERT INTO found_data_classifiers
SELECT 'TEST.public.employee'                           AS path_to_object,
       f.key                                            AS attribute,
       f.value:"privacy_category"::VARCHAR              AS privacy_category,  
       f.value:"semantic_category"::VARCHAR             AS semantic_category,
       f.value:"extra_info":"probability"::NUMBER(10,2) AS probability,
       'N'                                              AS deploy_flag,
       current_timestamp()::TIMESTAMP_NTZ               AS last_updated
FROM   TABLE(FLATTEN(extract_semantic_categories('TEST.public.employee')::VARIANT)) AS f
WHERE  privacy_category IS NOT NULL;

SELECT * FROM found_data_classifiers;

CREATE OR REPLACE PROCEDURE sp_classify_schema( P_DATABASE  STRING,
                                                P_SCHEMA    STRING ) RETURNS STRING
LANGUAGE javascript
EXECUTE AS CALLER
AS
$$
   var sql_stmt           = "";
   var stmt               = "";
   var outer_recset       = "";
   var inner_recset       = "";
   var result             = "";
   var debug_string       = '';
   sql_stmt  = "SELECT LOWER ( table_catalog )||'.'||\n";
   sql_stmt += "       LOWER ( table_schema  )||'.'||\n";
   sql_stmt += "       LOWER ( table_name    )\n";
   sql_stmt += "FROM   information_schema.tables\n";
   sql_stmt += "WHERE  table_catalog = UPPER ( :1 )\n"
   sql_stmt += "AND    table_schema  = UPPER ( :2 )\n"
   sql_stmt += "AND    table_type    IN ( 'BASE TABLE', 'MATERIALIZED VIEW' )\n"
   sql_stmt += "AND    table_owner IS NOT NULL;\n\n";
   debug_string += sql_stmt;
   stmt = snowflake.createStatement ({ sqlText:sql_stmt, binds:[P_DATABASE, P_SCHEMA] });
   try
   {
      outer_recset = stmt.execute();
      while(outer_recset.next())
      {
         sql_stmt  = `INSERT INTO found_data_classifiers\n`
         sql_stmt += `SELECT '` + outer_recset.getColumnValue(1) + `'         AS path_to_object,\n`
         sql_stmt += `       f.key                                            AS attribute,\n`
         sql_stmt += `       f.value:"privacy_category"::VARCHAR              AS privacy_category,\n`,  
         sql_stmt += `       f.value:"semantic_category"::VARCHAR             AS semantic_category,\n`,
         sql_stmt += `       f.value:"extra_info":"probability"::NUMBER(10,2) AS probability,\n`
         sql_stmt += `       'N'                                              AS deploy_flag,\n`
         sql_stmt += `       current_timestamp()::TIMESTAMP_NTZ               AS last_updated\n`
         sql_stmt += `FROM   TABLE(FLATTEN(extract_semantic_categories('` + outer_recset.getColumnValue(1) + `')::VARIANT)) AS f\n`
         sql_stmt += `WHERE  privacy_category IS NOT NULL;\n\n`
         debug_string += sql_stmt;

         stmt = snowflake.createStatement ({ sqlText:sql_stmt });
         try
         {
            inner_recset = stmt.execute();
         }
         catch { result = sql_stmt; }
      }
   }
   catch { result = sql_stmt; }
   return debug_string;
//   return result;
$$;

TRUNCATE TABLE found_data_classifiers;

CALL sp_classify_schema ( 'TEST',
                          'PUBLIC' );
                    
SELECT * FROM found_data_classifiers;

SELECT *
FROM   TABLE ( information_schema.tag_references_all_columns ( 'employee', 'TABLE' ));

USE ROLE securityadmin;

GRANT IMPORTED PRIVILEGES ON DATABASE snowflake TO ROLE sysadmin;

USE ROLE sysadmin;

CALL associate_semantic_category_tags
    ('TEST.public.employee',
     extract_semantic_categories('TEST.public.employee'));
     
SELECT *
FROM   TABLE ( information_schema.tag_references_all_columns ( 'employee', 'TABLE' ));

SELECT * FROM found_data_classifiers;

UPDATE found_data_classifiers
SET    deploy_flag    = 'Y'
WHERE  path_to_object = 'test.public.employee'
AND    attribute     != 'EMPLOYEE_ID';

CREATE OR REPLACE VIEW v_found_data_classifiers AS
SELECT path_to_object,
       '  "'||attribute||'": {'||
       '    "extra_info": {'||
       '      "alternates": [],'||
       '      "probability": "'||probability||'"'||
       '       },'||
       '    "privacy_category": "'||privacy_category||'",'||
       '    "semantic_category": "'||semantic_category||'"'||
       '    },'                                                   AS json_inner,
       deploy_flag
FROM   found_data_classifiers;

CREATE OR REPLACE VIEW v_found_data_classifiers_list AS
SELECT path_to_object,
       LISTAGG ( json_inner )                                     AS json_list,
       '{'||SUBSTR ( json_list, 1, LENGTH ( json_list ) -1 )||'}' AS json_source,
       to_json(parse_json ( json_source ))::VARIANT               AS json_string,
       deploy_flag
FROM   v_found_data_classifiers
GROUP BY path_to_object,
         deploy_flag;

SELECT *
FROM   v_found_data_classifiers_list;

SELECT to_json(parse_json ( json_string ))
FROM   v_found_data_classifiers_list
WHERE  deploy_flag    = 'Y'
AND    path_to_object = 'test.public.employee';


CALL associate_semantic_category_tags
    ('TEST.public.employee',
     ('{"COLLIBRA_TAG_ID":{"extra_info":{"alternates":[],"probability":"1"},"privacy_category":"QUASI_IDENTIFIER","semantic_category":"US_POSTAL_CODE"}}'));

CALL associate_semantic_category_tags
    ('TEST.public.employee',
     (SELECT json_string
      FROM   v_found_data_classifiers_list
      WHERE  deploy_flag = 'Y'
      AND    path_to_object = 'test.public.employee'));


/****************/
/* Data Lineage */
/****************/

SELECT *
FROM   TABLE ( information_schema.tag_references_all_columns ( 'employee', 'TABLE' ));

SELECT *
FROM   TABLE ( snowflake.account_usage.tag_references_with_lineage ( 'TEST.PUBLIC.PII_S_NAME' ));


/**************************/
/**** Remove Test Case ****/
/**************************
USE ROLE      accountadmin;
USE DATABASE  TEST;
USE WAREHOUSE test_wh;
USE SCHEMA    public;
 

DROP VIEW  v_tags;
DROP VIEW  v_tag_object;
DROP TABLE tag_object;
DROP TABLE tag;
DROP TABLE object;
DROP PROCEDURE sp_apply_object_tag();


ALTER TABLE employee UNSET TAG PII;
ALTER TABLE employee MODIFY COLUMN preferred_name     UNSET TAG PII_S_Name;
ALTER TABLE employee MODIFY COLUMN surname_preferred  UNSET TAG PII_S_Name;
ALTER TABLE employee MODIFY COLUMN forename_preferred UNSET TAG PII_S_Name;
ALTER TABLE employee MODIFY COLUMN gender             UNSET TAG PII_N_Gender;
 

DROP TAG PII;
DROP TAG PII_S_FullName;
DROP TAG PII_N_Gender;
*/

/******************/
/* tagadmin setup */
/******************/
USE ROLE securityadmin;

CREATE OR REPLACE ROLE tagadmin;

GRANT USAGE ON DATABASE  TEST        TO ROLE tagadmin;
GRANT USAGE ON WAREHOUSE compute_wh  TO ROLE tagadmin;
GRANT USAGE ON SCHEMA    TEST.public TO ROLE tagadmin;

GRANT CREATE TAG ON SCHEMA TEST.public TO ROLE tagadmin;

GRANT ROLE tagadmin TO USER john;

USE ROLE accountadmin;

GRANT APPLY TAG ON ACCOUNT TO ROLE tagadmin;





