/******************************************************************
 *
 * File:    Chapter_11_Query_Optimizer_Basics.txt
 *
 * Purpose: Sample query optimiser worked example
 *
 * Copyright: Andrew Carruthers - Building the Snowflake Data Cloud
 *
 ******************************************************************/

USE ROLE      sysadmin;
USE DATABASE  snowflake_sample_data;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    snowflake_sample_data.tpch_sf1000;

SELECT system$clustering_information ( 'lineitem' );

SELECT system$clustering_information ( 'partsupp' );

SELECT system$clustering_information ( 'nation' );

USE ROLE      sysadmin;
USE DATABASE  test;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    public;

CREATE TABLE test.public.partsupp_1
AS
SELECT * FROM snowflake_sample_data.tpch_sf1000.partsupp;

SELECT system$clustering_information ( 'partsupp_1' );

ALTER TABLE partsupp_1 CLUSTER BY (ps_supplycost > 100);

SELECT system$clustering_information ( 'partsupp_1' );

SELECT COUNT(DISTINCT ps_partkey) count_ps_partkey,
       COUNT(DISTINCT ps_suppkey) count_ps_suppkey
FROM   partsupp_1;

ALTER TABLE partsupp_1 CLUSTER BY (ps_suppkey, ps_partkey);

SELECT system$clustering_information ( 'partsupp_1' );

ALTER TABLE partsupp_1 CLUSTER BY (ps_suppkey);

SELECT system$clustering_information ( 'partsupp_1' );

SHOW TABLES LIKE 'partsupp_1';

ALTER TABLE partsupp_1 SUSPEND RECLUSTER;

ALTER TABLE partsupp_1 RESUME RECLUSTER;

SELECT *
FROM TABLE(information_schema.automatic_clustering_history
          ( date_range_start => dateadd ( H, -24, current_timestamp )));

/*********************/
/* Query Performance */
/*********************/
USE ROLE      sysadmin;
USE DATABASE  snowflake_sample_data;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA    snowflake_sample_data.tpcds_sf10tcl;

ALTER SESSION SET use_cached_result = FALSE;

ALTER WAREHOUSE compute_wh SET auto_suspend = 60;

SELECT i_product_name,
       SUM(cs_list_price)  OVER (PARTITION BY cs_order_number ORDER BY i_product_name ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) list_running_sum
FROM   catalog_sales, date_dim, item
WHERE  cs_sold_date_sk = d_date_sk
AND    cs_item_sk      = i_item_sk
AND    d_year IN (2000)
AND    d_moy  IN (1,2,3,4,5,6)
LIMIT 100;

ALTER WAREHOUSE compute_wh SET warehouse_size = 'SMALL';

ALTER WAREHOUSE compute_wh SET warehouse_size = 'MEDIUM';

ALTER WAREHOUSE compute_wh SET warehouse_size = 'LARGE';

ALTER WAREHOUSE compute_wh SET warehouse_size = 'X-LARGE';

ALTER WAREHOUSE compute_wh SET warehouse_size = 'X-SMALL';


ALTER WAREHOUSE compute_wh
SET   warehouse_size    = 'X-SMALL',
      max_cluster_count = 4
      min_cluster_count = 1
      scaling_policy    = STANDARD;


