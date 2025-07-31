--------------------------------
--Create Schema
--------------------------------
CREATE SCHEMA gold
----------------------------------
--Create View Customers Dimension
----------------------------------

CREATE VIEW gold.dim_customers
AS
SELECT 
    *
FROM 
    OPENROWSET
    (
        BULK 'https://datalakejani0.dfs.core.windows.net/gold/cust_dim/',
        FORMAT = 'DELTA'
    ) AS QUERy1

----------------------------------
--Create View Products Dimension
----------------------------------

CREATE VIEW gold.dim_products
AS
SELECT 
    *
FROM 
    OPENROWSET
    (
        BULK 'https://datalakejani0.dfs.core.windows.net/gold/prd_dim/',
        FORMAT = 'DELTA'
    ) AS QUERy2


-----------------------------
--Create View Sales Fact
------------------------------

CREATE VIEW gold.fact_sales
AS
SELECT 
    *
FROM 
    OPENROWSET
    (
        BULK 'https://datalakejani0.dfs.core.windows.net/gold/sales_fact/',
        FORMAT = 'DELTA'
    ) AS QUERy3
