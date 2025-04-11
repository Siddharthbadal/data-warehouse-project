-- Generating a report to show all key metrics of the data

SELECT 'Total Sales' AS measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL 
SELECT 'Total Quantity' AS mesaure_name, SUM(sales_quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price' AS mesaure_name, AVG(sls_price)  FROM gold.fact_sales
UNION ALL
SELECT 'Total No. Products' AS mesaure_name, COUNT(DISTINCT product_id) FROM gold.dim_products
UNION ALL 
SELECT 'Total No. Orders' AS mesaure_name, COUNT(DISTINCT order_number)  FROM gold.fact_sales
UNION ALL 
SELECT 'Total No Customers' AS mesaure_name, COUNT(customer_id) FROM gold.dim_customers
UNION ALL
SELECT 'Customers who placed order', COUNT(DISTINCT customer_key) FROM gold.fact_sales;