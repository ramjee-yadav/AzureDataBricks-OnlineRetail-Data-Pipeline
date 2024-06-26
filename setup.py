# Databricks notebook source
def create_database(db_name):
    print(f"Creating database: {db_name} ",end='')
    spark.sql("CREATE DATABASE IF NOT EXISTS {db_name}")

# COMMAND ----------

def create_sales_invoices():
    print(f"Creating sales_invoices table in bronze",end='')
    spark.sql("CREATE TABLE IF NOT EXISTS bronze.sales_invoices (
        InvoiceNo INT,
        StockCode STRING,
        Description STRING,
        Quantity INT,
        InvoiceDate DATE,
        UnitPrice DOUBLE,
        CustomerID INT,
        Country STRING,
        LoadTime DATE,
        SourceFile STRING )
         ")


# COMMAND ----------

def create_dim_customer():
    print(f"Creating dim_customer table in silver",end='')
    spark.sql("CREATE TABLE IF NOT EXISTS silver.dim_customer (
        customer_id INT,
        country STRING )
         ")

# COMMAND ----------

def create_dim_date():
    print(f"Creating dim_date table in silver",end='')
    spark.sql("CREATE TABLE IF NOT EXISTS silver.dim_date (
        datetime_id INT,
        datetime STRING,
        year INT,
        month INT,
        day INT,
        hour INT,
        minute INT,
        weekday INT )
         ")

# COMMAND ----------

def create_dim_product():
    print(f"Creating dim_product table in silver",end='')
    spark.sql("CREATE TABLE IF NOT EXISTS silver.dim_product (
        product_id INT,
        stock_code STRING,
        description STRING,
        price DOUBLE)
          ")

# COMMAND ----------

def create_fact_invoices():
    print(f"Creating fact_invoices table in silver",end='')
    spark.sql("CREATE TABLE IF NOT EXISTS silver.fact_invoices (
        invoice_id INT,
        datetime_id INT,
        product_id INT,
        customer_id INT,
        quantity INT,
        total DOUBLE )
         ")

# COMMAND ----------

# Create Databases
create_database('bronze')
create_database('silver')
create_database('gold')

# COMMAND ----------

# Create Bronze Tables
create_sales_invoices()

# Create Silver Tables
create_dim_customer()
create_dim_date()
create_dim_product()
create_fact_invoices()
