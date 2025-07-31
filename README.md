# Azure End To End Data Engineering Project
Welcome to the Azure End To End Data Engineering Project! 🚀

This project demonstrates development of an end to end data pipeline solution, from building a data warehouse from scratch to generating business ready data model using Microsoft Azure tools. The main goal was to bring all data into one system, clean it, apply business logic, and make it available for reporting and analytics.

---
## 🏗️ Data Architecture

The Microsoft Azure Resources utilized for this project are:
1. **Azure Data Factory (ADF)**: For data ingestion from source to destination.
2. **Azure Databricks**: For data transformations using spark.
3. **Azure Data Lake Storage Gen2 (ADLS)**: For storing all the data.
4. **Unity Catalog**: For data governance, access control, and schema management.
5. **Azure Synapse Analytics**: For reporting and querying
6. **Power BI**: For creating dashboards 


The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:
![Data Architecture](docs/data_architecture)

1. **Bronze Layer**: Stores raw data as parquet format from the source systems. Data is ingested from Git Repository to Azure Datalake Storage Gen 2 using Azure Data Factory Dynamic Pipelines.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis using Azure Databricks.
3. **Gold Layer**: Houses business-ready data modeled into a star schema required for reporting and analytics.

---
## 📖 Project Overview

This project involves:

1. **Data Architecture**: Designing a Modern Data Warehouse Using Medallion Architecture **Bronze**, **Silver**, and **Gold** layers.
2. **ETL Pipelines**: Extracting, transforming, and loading data from source systems into the warehouse.
3. **Data Modeling**: Developing fact and dimension tables optimized for analytical queries.

---

---

## 🚀 Project Requirements
### Building the Data Warehouse using End to End Pipeline (Data Engineering)
### Objective

Develop a modern data warehouse using Azure resources to consolidate sales data, enabling analytical reporting and informed decision-making.

### Specifications
- **Data Sources**: Import data from two source systems (ERP and CRM) provided as CSV files.
- **Data Quality**: Cleanse and resolve data quality issues prior to analysis.
- **Integration**: Combine both sources into a single, user-friendly data model designed for analytical queries.
- **Scope**: Focus on the latest dataset only; historization of data is not required.
- **Documentation**: Provide clear documentation of the data model to support both business stakeholders and analytics teams.


---

### 🛡️ License
This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

## 🌟 About Me
Hi there! I'm **Jani Basha Shaik**, an aspiring Data Engineer. Open to collaborating on projects, internships, or full-time roles. 

Feel free to connect with me on the following platforms:

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/259janibasha)
[![Gmail](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:janibashashaik917@gmail.com)
