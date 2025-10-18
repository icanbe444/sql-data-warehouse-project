USE DataWarehouse;
/* THIS Sub query help to check for duplicates in the joins
SELECT cst_id, COUNT(*) FROM 
(SELECT 
ci.cst_id,
ci.cst_key,
ci.cst_firstname,
ci.cst_lastname,
ci.cst_marital_status,
ci.cst_gndr,
ci.cst_create_date,
ca.bdate,
ca.gen,
la.cntry
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cus_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid
)t GROUP BY cst_id
HAVING COUNT(*) > 1
*/


-- cleaned the gender column
SELECT DISTINCT
ci.cst_gndr,
ca.gen,
CASE	WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'N/A')
END AS new_gen
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cus_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid


-- Change the field names to friendly names

SELECT 
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
ci.cst_marital_status AS marita_status,
CASE	WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'N/A')
END AS gender,
ci.cst_create_date AS create_date,
ca.bdate AS birthday,
la.cntry AS country
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cus_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid



-- change the order of the coumns
SELECT 
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marita_status,
CASE	WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'N/A')
END AS gender,
ca.bdate AS birthday,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cus_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid


-- Generate a surrogate key (New Primary Key) for the table using the window function ROW NUMBER
SELECT 
ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
ci.cst_id AS customer_id,
ci.cst_key AS customer_number,
ci.cst_firstname AS first_name,
ci.cst_lastname AS last_name,
la.cntry AS country,
ci.cst_marital_status AS marita_status,
CASE	WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
		ELSE COALESCE(ca.gen, 'N/A')
END AS gender,
ca.bdate AS birthday,
ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cus_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- CREATE AN OBJECT WHICH WILL BE A VIEW FOR THE GOLD LAYER
CREATE VIEW gold.dim_customers AS
SELECT 
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id AS customer_id,
	ci.cst_key AS customer_number,
	ci.cst_firstname AS first_name,
	ci.cst_lastname AS last_name,
	la.cntry AS country,
	ci.cst_marital_status AS marita_status,
	CASE	WHEN ci.cst_gndr != 'N/A' THEN ci.cst_gndr
			ELSE COALESCE(ca.gen, 'N/A')
	END AS gender,
	ca.bdate AS birthday,
	ci.cst_create_date AS create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cus_az12 ca
ON ci.cst_key = ca.cid
LEFT JOIN silver.erp_loc_a101 la
ON ci.cst_key = la.cid

-- check the qulity of the new object (View)
SELECT * FROM gold.dim_customers
SELECT DISTINCT gender FROM gold.dim_customers


-- Create the second dimension - Product
-- we have to sieve the historical data and retain only the current data
-- this we know by retaining only product with NULL as thier end date
-- product with end date suggest it is historical and not current data.
SELECT
pn.prd_id,
pn.cat_id,
pn.prd_key,
pn.prd_nm,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt,
pn.prd_end_dt,
pc.cat,
pc.subcat,
pc.maintenance
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL -- filters out all historical data


-- check to be sure the prouduct keys are unique
-- this should return nothing if it works
SELECT prd_key FROM( 
	SELECT
	pn.prd_id,
	pn.cat_id,
	pn.prd_key,
	pn.prd_nm,
	pn.prd_cost,
	pn.prd_line,
	pn.prd_start_dt,
	pn.prd_end_dt,
	pc.cat,
	pc.subcat,
	pc.maintenance
	FROM silver.crm_prd_info pn
	LEFT JOIN silver.erp_px_cat_g1v2 pc
	ON pn.cat_id = pc.id
	WHERE pn.prd_end_dt IS NULL
)t GROUP BY prd_key
HAVING COUNT(*) > 1


-- rearrange the columns
SELECT
pn.prd_id,
pn.prd_key,
pn.prd_nm,
pn.cat_id,
pc.cat,
pc.subcat,
pc.maintenance,
pn.prd_cost,
pn.prd_line,
pn.prd_start_dt
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL


-- give them a friendly name
SELECT
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS product_cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL

-- create a surrogate key for the dimension table
SELECT
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key )AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS product_cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL


-- create the view for the dimension
CREATE VIEW gold.dim_products AS
SELECT
ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key )AS product_key,
pn.prd_id AS product_id,
pn.prd_key AS product_number,
pn.prd_nm AS product_name,
pn.cat_id AS category_id,
pc.cat AS category,
pc.subcat AS subcategory,
pc.maintenance,
pn.prd_cost AS cost,
pn.prd_line AS product_line,
pn.prd_start_dt AS product_start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id = pc.id
WHERE pn.prd_end_dt IS NULL

-- create the fact table
-- We will create a data lookup to replace sls_prd_key and sls_cust_id 
-- with the surrogate keys created in the dimension tables
-- give the columns friendly names
-- create the view for Fact_sales TAble
CREATE VIEW gold.fact_sales AS
SELECT
sd.sls_ord_num AS order_number,
pr.product_key,
cu.customer_key,
sd.sls_order_dt AS order_date,
sd.sls_ship_dt AS shipping_daate,
sd.sls_due_dt AS due_date,
sd.sls_sales AS sales_amount,
sd.sls_quantity AS quantity,
sd.sls_price AS price
FROM silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key = pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id = cu.customer_id


-- check for issues in the tables. If it returns empty then everything matches

-- Test 1
SELECT 
* 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON C.customer_key = f.customer_key
WHERE c.customer_key IS NULL

--Test 2
SELECT 
* 
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON C.customer_key = f.customer_key
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
WHERE p.product_key IS NULL