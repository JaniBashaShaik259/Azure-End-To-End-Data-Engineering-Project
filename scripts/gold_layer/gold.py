# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Customer Dimension

# COMMAND ----------

df_cust_dim = spark.sql('''
        SELECT
            ROW_NUMBER() OVER (ORDER BY cst_id) AS customer_key, -- Surrogate key
            ci.cst_id                          AS customer_id,
            ci.cst_key                         AS customer_number,
            ci.cst_firstname                   AS first_name,
            ci.cst_lastname                    AS last_name,
            la.cntry                           AS country,
            ci.cst_marital_status              AS marital_status,
            CASE 
                WHEN ci.cst_gndr != 'n/a' THEN ci.cst_gndr -- CRM is the primary source for gender
                ELSE COALESCE(ca.gen, 'n/a')  			   -- Fallback to ERP data
            END                                AS gender,
            ca.bdate                           AS birthdate,
            ci.cst_create_date                 AS create_date
        FROM my_catalog.silver.crm_cust_info ci
        LEFT JOIN my_catalog.silver.erp_cust_az12 AS ca
            ON ci.cst_key = ca.cid
        LEFT JOIN my_catalog.silver.erp_loc_a101 AS la
            ON ci.cst_key = la.cid;
''')

# COMMAND ----------

df_cust_dim.write.format('delta')\
        .mode('overwrite')\
        .option('path', 'abfss://gold@datalakejani0.dfs.core.windows.net/cust_dim')\
        .saveAsTable('my_catalog.gold.cust_dim')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating Products Dimension

# COMMAND ----------

df_prd_dim = spark.sql('''
SELECT
    ROW_NUMBER() OVER (ORDER BY pn.prd_start_dt, pn.prd_key) AS product_key, -- Surrogate key
    pn.prd_id       AS product_id,
    pn.prd_key      AS product_number,
    pn.prd_nm       AS product_name,
    pn.cat_id       AS category_id,
    pc.cat          AS category,
    pc.subcat       AS subcategory,
    pc.maintenance  AS maintenance,
    pn.prd_cost     AS cost,
    pn.prd_line     AS product_line,
    pn.prd_start_dt AS start_date
FROM my_catalog.silver.crm_prd_info pn
LEFT JOIN my_catalog.silver.erp_px_cat_g1v2 pc
    ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL; -- Filter out all historical data
''')

# COMMAND ----------

df_prd_dim.write.format('delta')\
        .mode('overwrite')\
        .option('path', 'abfss://gold@datalakejani0.dfs.core.windows.net/prd_dim')\
        .saveAsTable('my_catalog.gold.prd_dim')


# COMMAND ----------

# MAGIC %md
# MAGIC ### Creating sales fact table

# COMMAND ----------

df_sales_fact = spark.sql('''
        SELECT
            sd.sls_ord_num  AS order_number,
            pr.product_key  AS product_key,
            cu.customer_key AS customer_key,
            sd.sls_order_dt AS order_date,
            sd.sls_ship_dt  AS shipping_date,
            sd.sls_due_dt   AS due_date,
            sd.sls_sales    AS sales_amount,
            sd.sls_quantity AS quantity,
            sd.sls_price    AS price
        FROM my_catalog.silver.crm_sales_details AS sd
        LEFT JOIN my_catalog.gold.prd_dim AS pr
            ON sd.sls_prd_key = pr.product_number
        LEFT JOIN my_catalog.gold.cust_dim AS cu
            ON sd.sls_cust_id = cu.customer_id;
''')

# COMMAND ----------

df_sales_fact.write.format('delta')\
        .mode('overwrite')\
        .option('path', 'abfss://gold@datalakejani0.dfs.core.windows.net/sales_fact')\
        .saveAsTable('my_catalog.gold.sales_fact')

# COMMAND ----------

