# Data Warehouse Project

Building a data warehouse with SQL server. Includes data modeling, analytics, and ETL process. A complete analytics solution, from building data warehouse to data analysis to find business insights.

## Objective
Goal is to develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

## Data Architecture
Follows the Medallion Architecture for this datawarehouse. Medallion Architecture is a data design pattern that organizes data into layers to improve data quality.

**Bronze Layer:** Stores raw data from the source system. Data is ingested from CSV files to bronze schema tables of the database in SQL Server. Data is checked for completeness and validations.

**Silver Layer:** This step is for data processing. Includes filtering, cleansing, standardization, and deduplication with normalization processes to prepare data for analysis. Sliver layer provides and Enterprise view of all its key business entities, concepts and transactions. 

**Gold Layer:** Data is further processed to turn into consumption-ready and project-specific data for reporting and analytics. This layer delivers continously updated and cleaned data to users and applications.

## Data Transformation
Process includes following steps:
-	Data Intregation
-	Data Enrichment
-	Derived Columns 
-	Data Normalization & Standarization and normalization
-	Business Rules and Logics 
-	Data Aggrestions 

## Data Cleaning:
-	Removing Duplicates
-	Data Filtering 
-	Handling Missing Data
-	Handling Invalid Values 
-	Data Type casting 
-	Finding Outlier 



