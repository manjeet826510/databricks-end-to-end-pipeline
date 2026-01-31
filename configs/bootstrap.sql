CREATE CATALOG databricks_cata;
USE CATALOG databricks_cata;
CREATE SCHEMA IF NOT EXISTS databricks_cata.silver;

-- CREATE TABLE IF NOT EXISTS databricks_cata.silver.orders_silver; --- already in pyspark code
-- CREATE TABLE IF NOT EXISTS databricks_cata.silver.customers_silver; --- already in pyspark code

CREATE OR REPLACE FUNCTION databricks_cata.bronze.discount_func(p_price DOUBLE)
RETURNS DOUBLE
LANGUAGE SQL
RETURN p_price * 0.90;

CREATE OR REPLACE FUNCTION databricks_cata.bronze.upper_func(p_brand STRING)
RETURNS STRING
LANGUAGE PYTHON
AS
$$
  return p_brand.upper()
$$;

-- CREATE TABLE IF NOT EXISTS databricks_cata.silver.products_silver; --- already in pyspark code
-- CREATE TABLE IF NOT EXISTS databricks_cata.silver.regions_silver; --- already in pyspark code



