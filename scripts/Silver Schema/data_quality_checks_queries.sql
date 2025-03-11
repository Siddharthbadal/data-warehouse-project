-- Queries for data quality checks 

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