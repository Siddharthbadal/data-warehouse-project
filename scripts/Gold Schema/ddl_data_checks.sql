-- check for dublicates values
SELECT cst_id, count(*)
FROM (
SELECT 
	ci.cst_id,
	ci.cst_key,
	ci.cst_firstname,
	ci.cst_lastname,
	ci.cst_material_status,
	ci.cst_gndr,
	ci.cst_create_date,
	ca.bdate,
	ca.gen,
	la.cntry
FROM silver.crm_cust_info ci 
LEFT JOIN silver.erp_cust_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la 
ON ci.cst_key = la.cid) t 
GROUP BY cst_id
HAVING COUNT(*) > 1;



SELECT prd_key, COUNT(*) 
FROM (
SELECT 
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL
) t GROUP BY prd_key
HAVING COUNT(*) > 1;

-- Forgein key intregity (Deminsion)
SELECT *
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key 
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE c.product_key IS NULL;