/*
	Create Database and Schemas

	Script Purpose:
		This script creates a new database 'DataWareHouse' if it does not exist.
		If database is already avilable, than it is recreated. 
		Scripts also sets up three new schemas for database. Bronze, Silver, Gold
	
	Warning:
		Running this script will drop the database if already exists. 
		This will lead to complete data loss.
*/

-- check for database and recreate
-- IF EXISTS (SELECT 1 FROM sys.databases WHERE = "DataWareHouse")
BEGIN
		ALTER DATABASE DataWareHouse SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
		DROP DATABASE DataWareHouse;
END;
GO


-- Create database 
CREATE DATABASE DataWareHouse;
USE DataWarehouse;

-- create schemas 
CREATE SCHEMA bronze;
GO
CREATE SCHEMA silver;
GO
CREATE SCHEMA gold;

