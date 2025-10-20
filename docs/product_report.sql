--USE DataWarehouse
/*
==============================================================================================================
Product Report
==============================================================================================================
Purpose:
	- This report consolidates key product metrics and behaviours

Highlights:
	1. Gathers essential fields such as product names, categories, subcategories and cost.
	2. Segments products by revenue to identify High-Performers, Mid-Range and Low-Performers.
	3. Aggregates product-level metrics:
		- total orders
		- total sales
		- total quantity purchased
		- total products
		- lifespan (in months)
	4. Calculates valuable KPIs:
		- recency (months since last order)
		- average order value (AOR)
		- average monthly revenue
===============================================================================================================
f.order_number,
	f.order_date,
	f.customer_key,
	f.quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost,
*/

IF OBJECT_ID('gold.report_products', 'V') IS NOT NULL
	DROP VIEW gold.report_products;
GO
CREATE VIEW gold.report_products AS
/*-------------------------------------------------------------------------------------------------------------
1). Base Query:Retrieves core columns from tables
--------------------------------------------------------------------------------------------------------------*/

WITH base_query AS(
SELECT 
	f.order_number,
	f.order_date,
	f.customer_key,
	f.sales_amount,
	f.quantity,
	p.product_key,
	p.product_name,
	p.category,
	p.subcategory,
	p.cost
FROM gold.fact_sales as f
LEFT JOIN gold.dim_products as p
ON f.product_key = p.product_key
WHERE order_date IS NOT NULL
),
product_aggregations AS (
 /*-------------------------------------------------------------------------------------------------------------
2). Product Aggregation: Summarizes key metrics at the product level
--------------------------------------------------------------------------------------------------------------*/

SELECT
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	DATEDIFF(MONTH, MIN(order_date), MAX(order_date)) AS lifespan,
	MAX(order_date) AS last_sale_date,
	COUNT(DISTINCT order_number) AS total_orders,
	COUNT(DISTINCT customer_key) AS total_customers, 
	SUM(sales_amount) AS total_sales,
	SUM(quantity) AS total_quantity,
	ROUND(AVG(CAST(sales_amount AS FlOAT)/NULLIF(quantity,0)), 1) as avg_selling_price
FROM base_query
GROUP BY product_key,
	product_name,
	category,
	subcategory,
	cost,
	quantity
)
 /*-------------------------------------------------------------------------------------------------------------
3). Final Query: Combines all product results into one output
--------------------------------------------------------------------------------------------------------------*/

SELECT 
	product_key,
	product_name,
	category,
	subcategory,
	cost,
	last_sale_date,
	DATEDIFF(MONTH, last_sale_date, GETDATE()) AS recency_in_months,
	CASE
		WHEN total_sales > 50000 THEN 'Higher Performer'
		WHEN total_sales >= 10000 THEN 'Mid-Range'
		ELSE 'Low Peformer'
	END AS product_segment,
	lifespan,
	total_orders,
	total_quantity,
	total_customers,
	avg_selling_price,
	-- Average Order Revenue
	CASE
		WHEN total_orders = 0 THEN 0
		ELSE total_orders / total_orders
	END AS avg_order_revenue,
	-- Average Monthly Revenue
	CASE
		WHEN lifespan = 0 THEN total_sales
		ELSE total_orders / lifespan
	END AS avg_monthly_revenue

FROM product_aggregations



