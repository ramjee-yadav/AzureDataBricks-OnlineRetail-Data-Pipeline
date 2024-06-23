# AzureDataBricks-OnlineRetail-Data-Pipeline
Online Retail Data-Pipeline with Azure Databricks and Delta lake

## Project Overview
This is a data engineering project using Azure Data bricks and Delta lake where I created a data pipeline to extract, transform and load to data warehouse. Which can be further analyzed and dashboard can be created using Power BI.

## Dataset
This is a transaction data set that contains UK based non-store online retail transactions.
[data set source](https://www.kaggle.com/datasets/tunguz/online-retail/data)

## Data model
![RetailOnline-Data-Model](/images/RetailOnline-Data-Model.jpg)

## Pipeline Architecture
![Data Pipeline Architecture](/images/OnlineRetail-Data-Pipeline-Architecture.png)

## Conclusion and Limitation
This project is having only basic implementation. We can add data quality checks and additional business logics as per the requirenment. This project can be implemented differently including different data stacks. For the basic demonstartion workflow is created in Databricks only. If we want a robust orchestartion and scheduling support we can use ADF also.
