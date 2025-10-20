
-- ==========================================================================================
-- Advanced Analytics 

-- Change over time analysis
-- Cumulative Analysis
-- Performance Analysis
-- Part to whole Analysis
-- Data Segmentation
-- ==========================================================================================

-- Change over time analysis (Analyse how measures evolve over time)

SELECT 
	YEAR(order_date),
	SUM(sales_amount),
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date)
ORDER BY YEAR(order_date)

SELECT 
	YEAR(order_date) AS order_year,
	MONTH(order_date) AS order_month,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY YEAR(order_date), MONTH(order_date)
ORDER BY YEAR(order_date),MONTH(order_date)


SELECT 
	DATETRUNC(month,order_date) AS order_year,
	SUM(sales_amount) AS total_sales,
	COUNT(DISTINCT customer_key) AS total_customers,
	SUM(quantity) AS total_quantity
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(month,order_date)
ORDER BY DATETRUNC(month,order_date)


-- Cumulative Analysis (Aggregating the data progressively over time)
-- It helps to show if business is growing or declining over time


-- Calculate the total sales per month
-- and the runnin total of sales over time

SELECT 
	order_date,
	total_sales,
	SUM(total_sales) OVER (ORDER BY order_date) AS running_total_sales,
	SUM(avg_price) OVER (ORDER BY order_date) AS moving_average_price
FROM
(
SELECT
	DATETRUNC(MONTH, order_date) AS order_date,
	SUM(sales_amount) as total_sales,
	AVG(price) AS avg_price
FROM gold.fact_sales
WHERE order_date IS NOT NULL
GROUP BY DATETRUNC(MONTH, order_date)
)t

/*
-- Performance Analysis (Comparing the current value to a target value)
-- It helps measure success and compare performance
-- Formular = current[measure] - target[measure]

-- Task
--Analyze the yearly performance of products by comparing each products sales 
--to both its avergae sales performance and the previous year's sales.
*/
WITH yearly_product_sales AS (
SELECT  
	YEAR(f.order_date) AS order_year, -- Change to month for a MOnth-Over-Month Analysis
	p.product_name,
	SUM(f.sales_amount) AS current_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON f.product_key = p.product_key
WHERE f.product_key IS NOT NULL
GROUP BY YEAR(f.order_date), p.product_name
)
SELECT
	order_year,
	product_name,
	current_sales,
	AVG(current_sales) OVER(PARTITION BY product_name) AS avg_sales,
	current_sales - AVG(current_sales) OVER(PARTITION BY product_name) AS diff_avg,
	CASE
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) > 0 THEN 'Increase'
		WHEN current_sales - AVG(current_sales) OVER(PARTITION BY product_name) < 0 THEN 'Decrease'
		ELSE 'Avg'
	End AS avg_change,
	-- Year-over-year Analysis
	LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS py_sales,
	current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) AS diff_py,
	CASE
		WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) > 0 THEN 'Above Avg'
		WHEN current_sales - LAG(current_sales) OVER (PARTITION BY product_name ORDER BY order_year) < 0 THEN 'Below Avg'
		ELSE 'No Change'
	End AS avg_change
FROM 
yearly_product_sales
ORDER BY product_name, order_year


-- Part to whole Analysis
-- Analyse how an individual part is performing compared to the overall allowing us to understand 
-- which category has the greatest impact on the business.
-- ([Measure]/Total[Measure]) * 100 by [Dimension]
-- E.g (Sales/totalSales) * 100 by Category
-- (Quantity/Total Quanity) * 100 by Country



-- Which category contribute the most the overall sales?
WITH category_sales AS (
SELECT
	p.category,
	SUM(f.sales_amount) AS total_sales
FROM gold.fact_sales f
LEFT JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY category
)
SELECT
	category,
	total_sales,
	SUM(total_sales) OVER() as overall_sales,
	CONCAT(ROUND((CAST(total_sales AS FLOAT)/ SUM(total_sales) OVER()) * 100, 2), '%') AS percentage_of_total
FROM category_sales
ORDER BY percentage_of_total DESC


-- Data Segmentation
--Group the data based on specific range
--Help understand the correlation between two measures
-- [Measure] by [Measure]
--Total Products by sales Range
--Total customers by Age

--TASK
-- Segment products into cost ranges and count how many proudcts faill into each segment.
WITH product_segment AS(
SELECT
	product_key,
	product_name,
	cost,
	CASE 
		WHEN cost < 100 THEN 'Below 100'
		WHEN cost BETWEEN 100 AND 500 THEN '100-500'
		WHEN cost BETWEEN 500 AND 1000 THEN '500-1000'
		ELSE 'Above 1000'
	END cost_range
FROM gold.dim_products)
SELECT 
	cost_range,
	COUNT(product_key) AS total_products
FROM product_segment
GROUP BY cost_range
ORDER BY total_products DESC


/*
Group customers into three segments based on their spending behaviour:
	- VIP: Customers with atleast 12 months of history and spending more than 5,000
	- Regular: Customers with at least 12 months of history but spenidng 5000 or less
	- New: Customers with a lifespan less than  months.
	And find the number of customers for each group
*/

WITH customer_spending AS(
SELECT
	c.customer_key,
	SUM(f.sales_amount) AS total_sales,
	MIN(order_date) AS first_order,
	MAX(order_date) AS last_order,
	DATEDIFF(month,MIN(order_date),MAX(order_date) ) AS life_span
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key=c.customer_key
GROUP BY c.customer_key
)
SELECT 
	customer_segment,
	COUNT(customer_key) AS total_customers
FROM(
SELECT
	customer_key,
	total_sales,
	life_span,
	CASE
		WHEN life_span >= 12 AND total_sales > 5000 THEN 'VIP'
		WHEN life_span >= 12 AND total_sales <= 5000 THEN 'Regular'
		ELSE 'New'
	END AS customer_segment
FROM customer_spending) T
GROUP BY customer_segment
ORDER BY total_customers


