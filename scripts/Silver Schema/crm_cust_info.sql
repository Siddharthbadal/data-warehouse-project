/*
===============================================================================
Load Silver Layer (Bronze -> Silver) | Stored Procedure
===============================================================================
Script Purpose:
    This stored procedure performs the ETL (Extract, Transform, Load) process to 
    populate the 'silver' schema tables from the 'bronze' schema.
	Actions Performed:
		- Truncates Silver tables.
		- Inserts transformed and cleansed data from Bronze schema into Silver schema tables.
	Table Transformations:
		- Drived columns from existing ones, Eg. Category ID, Product Key.
		- Map column codes to descriptiove values wherever applicable.
		- Calculating the end date based on startd dates.
		- Datatype casting whever required.
		- Handling missing data.
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/


-- Inserting values in to crm_cust_info
INSERT INTO silver.crm_cust_info (
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_material_status,
	cst_gndr,
	cst_create_date
)
SELECT 
	cst_id,
	cst_key,
	TRIM(cst_firstname) as cst_firstname,
	TRIM(cst_lastname) as cst_lastname, 
	CASE    
			WHEN  TRIM(cst_material_status) = 'S' THEN 'Single'
			WHEN  TRIM(cst_material_status) = 'M' THEN 'Married'
			ELSE 'n/a'
	END cst_material_status,
	CASE    
			WHEN  TRIM(cst_gndr) = 'F' THEN 'Female'
			WHEN  TRIM(cst_gndr) = 'M' THEN 'Male'
			ELSE 'n/a'
	END cst_gndr,
	cst_create_date
FROM (
	SELECT *,
			ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
		FROM bronze.crm_cust_info
		WHERE cst_id IS NOT NULL
) as t 
	WHERE flag_last = 1;


-- Inserting values into prd_crm

INSERT INTO silver.crm_prd_info(
	prd_id,
	cat_id,
	prd_key,
	prd_nm,
	prd_cost,
	prd_line,
	prd_start_dt,
	prd_end_dt
)
SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-','_') AS cat_id,
		SUBSTRING(prd_key, 1, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE UPPER(TRIM(prd_line)) 
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'n/a'
		END AS prd_line,				
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info;






