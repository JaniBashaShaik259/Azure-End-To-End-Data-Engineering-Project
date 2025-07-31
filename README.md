# Azure End To End Data Engineering Project
Welcome to the Azure End To End Data Engineering Project! 🚀

This project demonstrates development of an end to end data pipeline solution, from building a data warehouse from scratch to generating business ready data model using Microsoft Azure tools. The main goal was to bring all data into one system, clean it, apply business logic, and make it available for reporting and analytics.

---
## 🏗️ Data Architecture

The Microsoft Azure Resources utilized for this project are:
1. **Azure Data Factory (ADF)**: For data ingestion from source to destination.
2. **Azure Databricks**: For data transformations using PySpark and Spark-SQL.
3. **Azure Data Lake Storage Gen2 (ADLS)**: For storing all the data.
4. **Unity Catalog**: For data governance, access control, and schema management.
5. **Azure Synapse Analytics**: For reporting and querying
6. **Power BI**: For creating dashboards (Established connection between Synapse and Power BI)


![Data Architecture](docs/data_architecture)
The data architecture for this project follows Medallion Architecture **Bronze**, **Silver**, and **Gold** layers:

1. **Bronze Layer**: Stored raw data as parquet format from the source systems. Data is ingested from Git Repository to Azure Datalake Storage Gen 2 using Azure Data Factory Dynamic Pipelines.
2. **Silver Layer**: This layer includes data cleansing, standardization, and normalization processes to prepare data for analysis using Azure Databricks PySpark and Spark-SQL. Stored tranformed data in Delta format under the silver schema.
3. **Gold Layer**: Created final fact and dimension tables (e.g., sales, customer, product). Saved final data under the gold schema. Houses business-ready data modeled into a star schema required for BI and Data Science teams.


---

This project helped business leaders track how their prices and prodcuts are performing across regions.
Made it easy for data anylysts and scientists to use clean data for creating dashboards and ML models.


---

### 🛡️ License
This project is licensed under the [MIT License](LICENSE). You are free to use, modify, and share this project with proper attribution.

## 🌟 About Me
Hi there! I'm **Jani Basha Shaik**, an aspiring Data Engineer. Open to collaborating on projects, internships, or full-time roles. 

Feel free to connect with me on the following platforms:

[![LinkedIn](https://img.shields.io/badge/LinkedIn-0077B5?style=for-the-badge&logo=linkedin&logoColor=white)](https://www.linkedin.com/in/259janibasha)
[![Gmail](https://img.shields.io/badge/Gmail-D14836?style=for-the-badge&logo=gmail&logoColor=white)](mailto:janibashashaik917@gmail.com)
