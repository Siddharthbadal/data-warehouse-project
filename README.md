# Data Warehouse and Analytics Project

Building a data warehouse with SQL server. Includes data modeling, analytics, and ETL process. A complete analytics solution, from building data warehouse to data analysis to find business insights.

## Objective
Goal is to develop a modern data warehouse using SQL Server to consolidate sales data, enabling analytical reporting and informed decision-making.

### Data Architecture
Follows the Medallion Architecture for this datawarehouse. Medallion Architecture is a data design pattern that organizes data into layers to improve data quality.

**Bronze Layer:** Stores raw data from the source system. Data is ingested from CSV files to bronze schema tables of the database in SQL Server. Data is checked for completeness and validations.

**Silver Layer:** This step is for data processing. Includes filtering, cleansing, standardization, and deduplication with normalization processes to prepare data for analysis. Sliver layer provides and Enterprise view of all its key business entities, concepts and transactions. 

**Gold Layer:** Data is further processed to turn into consumption-ready and project-specific data for reporting and analytics. This layer delivers continously updated and cleaned data to users and applications.

![datawarehouse](https://raw.githubusercontent.com/Siddharthbadal/data-warehouse-project/refs/heads/main/images/datawarehouse.png)

### Process Followed
-   SQL Development, Database Desgin and Development
-   Data Architect 
-   ETL
-   Data Enginerring 
-   Data Modeling
-   Data Analytics

**Data Sources**: Import data from CSV files.

**Data Quality**: Clean and manage quality for data analysis.

**Intregation**: Combine data sources into a single data model designed for analysis.

**Documentation**: Provide clear documentation of the data model to support business and analytics teams.


For our datawarehouse, we created stored procedures for both bronze and silver layer.
In gold layer process, we combine the related data from all tables to create facts and deminension table views for further data analysis. 

### Data Transformation Under Silver Layer
Process includes following steps:
-   Data Cleansing
-   Data Normalization
-   Data Standarization
-	Data Enrichment
-	Derived Columns 

### Data Cleaning:
-	Removing Duplicates
-	Data Filtering 
-	Handling Missing Data
-	Handling Invalid Values 
-	Data Type casting 
-	Finding Outlier 

### Data Transformation Under Gold Layer
-	Data Intregation
-	Data Aggreations 
-	Business Rules and Logics 

### Data Model in Gold Schema
-   Star Schema
-   Flat table
-   Aggregated table



