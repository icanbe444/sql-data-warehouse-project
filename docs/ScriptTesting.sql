USE DataWarehouse

-- Check for Null or Duplicates in Primary Key
-- Expectations: No Result

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*) > 1 OR prd_id IS NULL;


-- Check for unwanted spaces
-- Expectation: No Results
SELECT 
prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm);

-- Check for NULLS or Negative Numbers
-- Expectations: NO Results
-- If they are, replace with zero

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 or prd_cost IS NULL


-- Check the distinct values in prd_line
-- Replace the letters with friendly values
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for invalid date orders
SELECT 
prd_id,
prd_key,
prd_nm,
prd_cost,
prd_start_dt,
prd_end_dt,
LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt)-1 AS prd_end_dt_test
FROM bronze.crm_prd_info
WHERE prd_key IN ('AC-HE-HL-U509-R','AC-HE-HL-U509');


SELECT
*
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

SELECT
*
FROM silver.crm_prd_info