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
	Table Transformations (Wherever applicable):
		- Drived columns from existing ones, Eg. Category ID, Product Key.
		- Map column codes to descriptiove values wherever applicable.
		- Calculating the end date based on startd dates.
		- Datatype casting.
		- Handling missing data.
		- Handling invalid data.
		- Maintaing data standardization & consistency 
Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC Silver.load_silver;
===============================================================================
*/

CREATE OR ALTER PROCEDURE silver.load_silver_data AS
BEGIN
DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME

	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '--------------------------------------';
		PRINT 'Loading Silver Layer For DatawareHouse';
		PRINT '--------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------';

		
	-- Inserting values in to crm_cust_info
	PRINT '>> Truncating crm cust info table'
	TRUNCATE TABLE silver.crm_cust_info;
	PRINT '>> Inseting data into crm cust info table'
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
	PRINT '>> Truncating crm prod info table'
	TRUNCATE TABLE silver.crm_prd_info;
	PRINT '>> Inseting data into crm prod info table'
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


	-- Insert data from crm details table
	PRINT '>> Truncating crm sales detail table'
	TRUNCATE TABLE silver.crm_sales_details;
	PRINT '>> Inseting data into crm sales details table'
	INSERT INTO silver.crm_sales_details(
		sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	SELECT 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	-- check for invalid dates and datataype casting
	CASE WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL 
		 ELSE CAST(CAST(sls_order_dt AS varchar) AS DATE)
	END AS sls_order_dt, 
	CASE WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL 
		 ELSE CAST(CAST(sls_ship_dt AS varchar) AS DATE)
	END AS sls_ship_dt,
	CASE WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL 
		 ELSE CAST(CAST(sls_due_dt AS varchar) AS DATE)
	END AS sls_due_dt,
	-- checks sales figure, quantity, and using business logic to get correct data
	CASE WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
			THEN sls_quantity * ABS(sls_price)
		 ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE WHEN sls_price IS NULL OR sls_price <= 0
			THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
	FROM bronze.crm_sales_details;



	-- erp customer table
	PRINT '--------------------------------------';
	PRINT 'Loading ERP Tables';
	PRINT '--------------------------------------';

	PRINT '>> Truncating erp customer table'
	TRUNCATE TABLE silver.erp_cust_az12;
	PRINT '>> Inseting data into erp customer table'
	INSERT INTO silver.erp_cust_az12(cid, bdate, gen)
	SELECT
	-- checking for valid ids and removing extra characters
	CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4, LEN(cid))
		ELSE cid 
	END AS cid,
	-- checking and removing future dates
	CASE WHEN bdate > GETDATE() THEN NULL 
		ELSE bdate 
	END AS bdate,
	-- normalizing and keeping identical entires for gender
	CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		 ELSE 'n/a'
	END AS gen
	from bronze.erp_cust_az12;

	-- erp location table 
	PRINT '>> Truncating erp location table'
	TRUNCATE TABLE silver.erp_loc_a101;
	PRINT '>> Inseting data into erp location table'
	INSERT INTO silver.erp_loc_a101(cid, cntry)
	select  distinct
		-- check for invalid values
		REPLACE(cid, '-', '') AS cid,
	-- normalize and handling missing values and standarize the values
	CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
		 ELSE TRIM(cntry)
	END cntry
	from bronze.erp_loc_a101;

	-- category table
	PRINT '>> Truncating erp category table'
	TRUNCATE TABLE silver.erp_px_cat_g1v2;
	PRINT '>> Inseting data into erp category table'
	INSERT INTO silver.erp_px_cat_g1v2(id, cat, subcat, maintenance)
	SELECT 
		id,
		cat,
		subcat,
		maintenance
	FROM bronze.erp_px_cat_g1v2;
	SET @batch_end_time = GETDATE();
		PRINT '-------------------------';
		PRINT 'Silver layer upload completed.';
		PRINT '>> Total Load Duration: '+ CAST(DATEDIFF(second, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds.';
		PRINT '-------------------------';
	END TRY
	BEGIN CATCH
		PRINT '----------------------';
		PRINT 'Eror occured during loading bronze layer';
		PRINT 'Error Mesage' + ERROR_MESSAGE();
		PRINT 'Error Mesage' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Mesage' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '-----------------------';

	END CATCH
END
-- execute silver layer data
exec silver.load_silver_data;