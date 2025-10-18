USE DataWarehouse;

/*
======================================================================================================
Cleaning, normalizing and standardizing the crm_cust_info table
======================================================================================================
We need to remove duplicates in teh cst_id
We check the cst_id one after the other and check the creation date to pick the 
newest data or the row with the most complete data. This is important to remove the duplicates
*/

SELECT
*
FROM(
SELECT 
*,
ROW_NUMBER () OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;



-- Check for unwanted spaces in the columns
-- Expectations = NO Results

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) cst_lastname,
cst_marital_status,
cst_gndr
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER () OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;


-- Check for Data Standardization & Consistency in gender
-- Change F to Female, M to Male

SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info;

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) cst_lastname,
cst_marital_status,
CASE	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'N/A'
END cst_gndr,
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER () OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;


-- Check for Data Standardization & Consistency in marital status
-- Change F to Female, M to Male

SELECT DISTINCT cst_marital_status FROM bronze.crm_cust_info;

SELECT 
cst_id,
cst_key,
TRIM(cst_firstname) AS cst_firstname,
TRIM(cst_lastname) cst_lastname,
CASE	WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN  UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'N/A'
END cst_marital_status,
CASE	WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'N/A'
END cst_gndr,
cst_create_date
FROM (
SELECT 
*,
ROW_NUMBER () OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last
FROM bronze.crm_cust_info
)t WHERE flag_last = 1;


/*
======================================================================================================
Cleaning, normalizing and standardizing the crm_prd_info table
======================================================================================================
We need to check for duplicates in the prd_id
derive cat_id and prd_key from the column prd_key
Check if the dates are in order (USE THE LEAD AND LAG WINDOW FUNCTION TO FIX)
*/

SELECT
prd_id
FROM bronze.crm_prd_info;

-- Remove the first 5 characters from the prd_key and 
-- replace the '-' with '_' so it matches with id from erp_px_cat_g1v2

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info;


-- Compare the 5 characters with the id in erp_px_cat_g1v2

SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2

-- Derived columns: cat_id
-- Check if the prd_keys match what we have in erp_px_cat_g1v2
-- Check if the new substring exist in the list of keys in erp_px_cat_g1v2

SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE REPLACE(SUBSTRING(prd_key,1,5), '-', '_') NOT IN 
(SELECT DISTINCT id FROM bronze.erp_px_cat_g1v2);




-- Derived columns: prd_key
-- Compare the new prd_key with the sales product key (sls_prd_key)
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
prd_nm,
prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7, LEN(prd_key)) NOT IN 
(SELECT sls_prd_key FROM bronze.crm_sales_details);

-- Replace the NULL values in prd_cost with zero
SELECT
prd_id,
prd_key,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7, LEN(prd_key)) NOT IN 
(SELECT sls_prd_key FROM bronze.crm_sales_details);

-- Replace the letters in prd_line with friendly values
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
END prd_line,
prd_start_dt,
prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7, LEN(prd_key)) NOT IN 
(SELECT sls_prd_key FROM bronze.crm_sales_details);



-- Fix the date orders (There are rows where end dates preceed the start date)
-- Remove the time in the date as there is no information in there
SELECT
prd_id,
REPLACE(SUBSTRING(prd_key,1,5), '-', '_') AS cat_id,
SUBSTRING(prd_key,7, LEN(prd_key)) AS prd_key,
prd_nm,
ISNULL(prd_cost,0) AS prd_cost,
CASE UPPER(TRIM(prd_line))
		WHEN 'M' THEN 'Mountain'
		WHEN 'R' THEN 'Road'
		WHEN 'S' THEN 'Other Sales'
		WHEN 'T' THEN 'Touring'
		ELSE 'N/A'
END prd_line,
CAST(prd_start_dt AS DATE) AS prd_start_dt,
CAST(LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt )-1 AS DATE) AS prd_end_dt
FROM bronze.crm_prd_info
WHERE SUBSTRING(prd_key,7, LEN(prd_key)) NOT IN 
(SELECT sls_prd_key FROM bronze.crm_sales_details);



/*
======================================================================================================
Cleaning, normalizing and standardizing the crm_sales_details table
======================================================================================================
We need to check for duplicates in the prd_id
*/

-- Check for trailing and leading spaces in the sls_ord_num
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details
WHERE sls_ord_num != TRIM(sls_ord_num);

-- Check if all product keys exist in the product info
-- Check if all cust_id matches what we have in the customer table 
SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details


-- Fix the date column
-- Change the numbers to date format
-- check for zero values in the order date, shipping and due date
-- convert the dates to date format
SELECT
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0

-- fIX the date by checking if all match (YYYYMMDD)
SELECT
NULLIF(sls_order_dt, 0) sls_order_dt
FROM bronze.crm_sales_details
WHERE sls_order_dt <= 0 OR LEN(sls_order_dt) != 8;


SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
FROM bronze.crm_sales_details

-- check for invalid date order as order date must preceeds shipping date

SELECT
	sls_order_dt,
	sls_ship_dt
FROM silver.crm_sales_details
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;


-- check the sales, quantity and price
SELECT DISTINCT
	sls_sales,
	sls_quantity,
	sls_price
FROM silver.crm_sales_details
WHERE sls_sales != sls_price * sls_quantity 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <= 0 OR sls_quantity <= 0 OR sls_price <=0
ORDER BY sls_sales, sls_quantity, sls_price;

 

SELECT DISTINCT
	sls_sales AS oldsales,
	sls_quantity,
	sls_price AS oldprice,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	CASE
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details;


SELECT
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	CASE	WHEN sls_order_dt = 0 OR LEN(sls_order_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
	END AS sls_order_dt,
	CASE	WHEN sls_ship_dt = 0 OR LEN(sls_ship_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
	END AS sls_ship_dt,
	CASE	WHEN sls_due_dt = 0 OR LEN(sls_due_dt) != 8 THEN NULL
			ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
	END AS sls_due_dt,
	CASE
		WHEN sls_sales IS NULL OR sls_sales <= 0 OR sls_sales != sls_quantity * ABS(sls_price)
		THEN sls_quantity * ABS(sls_price)
		ELSE sls_sales
	END AS sls_sales,
	sls_quantity,
	CASE
		WHEN sls_price IS NULL OR sls_price <= 0
		THEN sls_sales / NULLIF(sls_quantity,0)
		ELSE sls_price
	END AS sls_price
FROM bronze.crm_sales_details



/*
======================================================================================================
Cleaning, normalizing and standardizing the erp_cust_az12 table
======================================================================================================
We need to check for duplicates in the prd_id
*/
-- It appears the cst_id in erp_cust_az12 is slightly diffent from the structure in crm_cust_info
-- we need to fix that by matching the two columns.

SELECT 
	cid,
	bdate,
	gen
FROM bronze.erp_cus_az12
WHERE cid LIKE '%AW00011000%';


SELECT * FROM silver.crm_cust_info;

-- We need to remove the leading 'NAS' in the cid

SELECT 
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
		ELSE cid
	END AS cid,
	bdate,
	gen
FROM bronze.erp_cus_az12;


-- Check if the date is in range and in date format

SELECT 
	bdate
FROM silver.erp_cus_az12
WHERE bdate < '1924-01-01' OR bdate > GETDATE();


SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	gen
FROM bronze.erp_cus_az12;


-- check the distict values in gen column

SELECT
	CASE 
		WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4,LEN(cid))
		ELSE cid
	END AS cid,
	CASE 
		WHEN bdate > GETDATE() THEN NULL
		ELSE bdate
	END AS bdate,
	CASE
		WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
		WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
		ELSE 'N/A'
	END AS gen
FROM bronze.erp_cus_az12;

SELECT * FROM silver.erp_cust_az12


/*
======================================================================================================
Cleaning, normalizing and standardizing the erp_loc_a101 table
======================================================================================================

*/

-- there is an hyphen between cid which is not present in the cid we have in the customer table.
-- this needs to be removed.

SELECT 
	REPLACE(cid, '-', '')
	cid,
	cntry
FROM bronze.erp_loc_a101;


-- check the cntry column for distinct values

SELECT DISTINCT
	cntry
FROM bronze.erp_loc_a101;

-- the column has invalid values and null values and needs to be fixed

SELECT
	REPLACE(cid, '-', ''),
	CASE
		WHEN TRIM(cntry) = 'DE' THEN 'Germany'
		WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
		WHEN TRIM(cntry) = '' OR TRIM(cntry) IS NULL THEN 'N/A'
		ELSE TRIM(cntry)
	END AS cntry
FROM bronze.erp_loc_a101;



/*
======================================================================================================
Cleaning, normalizing and standardizing the bronze.erp_px_cat_g1v2 table
======================================================================================================

*/

SELECT 
	id,
	cat,
	subcat,
	maintenance
FROM bronze.erp_px_cat_g1v2

-- check for unwanted spaces in the cat column
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE cat != TRIM(cat); 


-- check for unwanted spaces in the subcat column
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE subcat != TRIM(subcat); 

-- check for unwanted spaces in the maintenance column
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE maintenance != TRIM(maintenance); 

-- check for Data Standardization & Consistency

SELECT DISTINCT
cat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
subcat
FROM bronze.erp_px_cat_g1v2

SELECT DISTINCT
maintenance
FROM bronze.erp_px_cat_g1v2