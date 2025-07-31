CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Bismillaah@123' ;
GO

CREATE DATABASE SCOPED CREDENTIAL jani_creds
WITH 
        IDENTITY = 'Managed Identity' ;
GO

CREATE EXTERNAL DATA SOURCE source_gold
WITH
(
    LOCATION = 'https://datalakejani0.blob.core.windows.net/gold',
    CREDENTIAL = jani_creds
);
GO

CREATE EXTERNAL DATA SOURCE source_gold_ext
WITH
(
    LOCATION = 'https://datalakejani0.blob.core.windows.net/gold-ext',
    CREDENTIAL = jani_creds
);
GO


CREATE EXTERNAL FILE FORMAT format_parquet
WITH (
    FORMAT_TYPE = PARQUET,
    DATA_COMPRESSION = 'org.apache.hadoop.io.compress.SnappyCodec'
);
GO


----------------------------------------------
--Create External Table ext_dim_cust
----------------------------------------------

CREATE EXTERNAL TABLE gold.ext_dim_cust
WITH
(
    LOCATION = 'ext_dim_cust',
    DATA_SOURCE = source_gold_ext,
    FILE_FORMAT = format_parquet
)AS
SELECT * FROM gold.dim_customers;
GO

CREATE EXTERNAL TABLE gold.ext_dim_prd
WITH
(
    LOCATION = 'ext_dim_prd',
    DATA_SOURCE = source_gold_ext,
    FILE_FORMAT = format_parquet
)AS
SELECT * FROM gold.dim_products;
GO

CREATE EXTERNAL TABLE gold.ext_fact_sales
WITH
(
    LOCATION = 'ext_fact_sales',
    DATA_SOURCE = source_gold_ext,
    FILE_FORMAT = format_parquet
)AS
SELECT * FROM gold.fact_sales;
