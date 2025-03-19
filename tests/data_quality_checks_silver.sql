/*
	Quality Checks
	This script is for silver layer to perform various quality checks 
	on data for accuracy, validations, consistency and standardization on data
	from bronze layer.  
	-	Unwanted spaces
	-	Null or blank values
	-	Invalid dates
	-	Standardized data accross column.
*/

-- finding duplicate values
SELECT 
	cst_id, COUNT(*)
from bronze.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- check for traling/leading spaces in the columns
SELECT cst_firstname from bronze.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

-----
SELECT 
	prd_id, COUNT(*)
from bronze.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- check for spaces
SELECT *
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- check for negative and null numbers
SELECT * 
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS null;

-- Data standardization and consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- invalid date orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- sales details table 
SELECT *
FROM bronze.crm_sales_details 
where sls_order_dt <=0
OR LEN(sls_order_dt) != 8
OR sls_order_dt > 20250101
OR sls_order_dt > 20000101

SELECT *
FROM bronze.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

SELECT sls_sales, sls_quantity , sls_price
FROM bronze.crm_sales_details
WHERE sls_sales != sls_quantity * sls_price
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 or sls_quantity <= 0 OR sls_price <= 0
ORDER by sls_sales, sls_quantity , sls_price

/*
	Sales rules:
		if sales is negative, zero or null drive it with quantity and price
		if price is zero or null, find it by sales and quantity 
		if price is negative, convert values to positive
*/


-- erp tables

SELECT *
FROM bronze.erp_cust_az12
where cid LIKE 'NAS%'

select * 
from bronze.erp_cust_az12
where bdate < '1925-01-01' OR bdate > GETDATE();

select distinct gen,
CASE WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
	 WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
	 ELSE 'n/a'
END AS gen
from bronze.erp_cust_az12;


select
	REPLACE(cid, '-', '') AS cid, cntry
from bronze.erp_loc_a101;


select  distinct
	cntry as old,
CASE WHEN TRIM(cntry) = 'DE' THEN 'Germany'
	 WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
	 WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
	 ELSE TRIM(cntry)
END cntry
from bronze.erp_loc_a101;


select *
from bronze.erp_px_cat_g1v2;
-- unwanted spaces
select *
from bronze.erp_px_cat_g1v2
where cat != TRIM(cat) OR subcat != TRIM(subcat) OR maintenance != TRIM(maintenance)