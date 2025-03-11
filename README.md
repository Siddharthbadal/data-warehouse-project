# Data Warehouse Project

Building a data warehouse with SQL server. Includes data modeling, analytics, and ETL process. A complete data ware housing and analytics solution, from building data warehouse to data analysis to find business insights.

## Data Architecture
The data architecture for building this dataware we follow the Medallion Architecture. Medallion Architecture is a data design pattern that organizes data inti layers to improve data quality.
	-	Bronze Layer: Stores raw data from the source system. Data is ingested from CSV files to bronze schema tables of the database in SQL Server. Data is checked for completeness and validations.
	-	Silver Layer: This step is for data processing. Includes filtering, cleansing, standardization, and deduplication with normalization processes to prepare data for analysis. Sliver layer provides and Enterprise view of all its key business entities, concepts and transactions. 
	-	Gold Layer: Data is further processed to turn into consumption-ready and project-specific data for reporting and analytics. This layer delivers continously updated and cleaned data to users and applications.
