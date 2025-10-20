--Explore all objects in the Database
SELECT * FROM INFORMATION_SCHEMA.TABLES


-- Explore all columns in teh database
SELECT * FROM INFORMATION_SCHEMA.COLUMNS
WHERE TABLE_NAME = 'dim_customers'


-- ==========================================================================================
-- Explorattory Data Analysis
-- ==========================================================================================

-- Explore all countries our customers come from
SELECT DISTINCT country FROM gold.dim_customers

--Dimension Values
-- Explore all categories 'The major divisions
SELECT DISTINCT category, subcategory, product_name FROM gold.dim_products
ORDER BY 1,2,3

-- Find teh date of the first and last order
-- How many years of sales are available in our data

SELECT 
MIN(order_date) AS first_order_date,
MAX(order_date) AS last_order_date,
DATEDIFF(month, MIN(order_date),MAX(order_date)) AS order_range_months
FROM gold.fact_sales


--Find the youngest and the olderst customer
SELECT
MIN(birthday)AS oldest_birthday,
DATEDIFF(Year,MIN(birthday),GETDATE()) AS birthday_range_year,
MAX(birthday) AS youngest_birthday,
DATEDIFF(Year,MAX(birthday),GETDATE()) AS birthday_range_year
FROM gold.dim_customers 

--============================================================
--Exploration of the measures
--============================================================
-- Find the Total sales 
SELECT 
SUM(sales_amount) AS total_sales
FROM gold.fact_sales

-- Find how many items are sold
SELECT 
SUM(quantity) AS total_quantity
FROM gold.fact_sales

-- Find the average selling price
SELECT  AVG(price) AS average_price FROM gold.fact_sales;


-- Find the Total number of orders
SELECT COUNT(order_number) AS total_orders FROM gold.fact_sales;
SELECT COUNT(DISTINCT order_number) AS total_orders FROM gold.fact_sales;-- counting distinct number of orders


-- Find the Total number of products
SELECT COUNT(product_key) AS total_products FROM gold.dim_products;
SELECT COUNT(DISTINCT product_key) AS total_products FROM gold.dim_products;
SELECT COUNT(DISTINCT product_name) AS total_products FROM gold.dim_products;

-- Find the Total number of customers
SELECT COUNT(customer_key) AS total_customers FROM gold.dim_customers; 

-- Find the total number of customers that has placed an order
SELECT COUNT(DISTINCT customer_key) AS total_customers FROM gold.fact_sales; 

--Generate report that shows all key metrics of the business
SELECT 'Total Sales' as measure_name, SUM(sales_amount) AS measure_value FROM gold.fact_sales
UNION ALL
SELECT 'Total Quantity', SUM(quantity) FROM gold.fact_sales
UNION ALL
SELECT 'Average Price', AVG(price) FROM gold.fact_sales
UNION ALL 
SELECT 'Total No. of Orders', COUNT(DISTINCT order_number) FROM gold.fact_sales
UNION ALL
SELECT 'Total No. of Products',COUNT(DISTINCT product_name) FROM gold.dim_products
UNION ALL
SELECT 'Total No. of Customers', COUNT(customer_key) FROM gold.dim_customers


-- ==========================================================================================
-- Magnitude Analysers (comparing the measures values by categories 
-- ==========================================================================================

-- Find total number of customers by countries
SELECT 
	country,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY country
ORDER BY total_customers DESC

-- Find total customers by gender
SELECT 
	gender,
	COUNT(customer_key) AS total_customers
FROM gold.dim_customers
GROUP BY gender
ORDER BY total_customers DESC

-- Find total product by category
SELECT
	category,
	COUNT(product_key) AS total_product
FROM gold.dim_products
GROUP BY category
ORDER BY total_product DESC

-- What is the average cost in each category?
SELECT
	category,
	AVG(cost) AS average_cost
FROM gold.dim_products
GROUP BY category
ORDER BY average_cost DESC

-- What is the total revenue generated for each category?
SELECT
	pr.category,
	SUM(fs.sales_amount) AS total_revenue
FROM gold.fact_sales fs
LEFT JOIN gold.dim_products pr
ON fs.product_key = pr.product_key
GROUP BY pr.category
ORDER BY total_revenue DESC

-- Find total revenue generated from each customer
SELECT
	c.customer_key,
	c.first_name,
	c.last_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.customer_key, c.first_name,c.last_name
ORDER BY total_revenue DESC


-- What is the distribution of sold items across countries?
SELECT
	c.country,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
LEFT JOIN gold.dim_customers c
ON f.customer_key = c.customer_key
GROUP BY c.country
ORDER BY total_revenue DESC


-- ==========================================================================================
-- Ranking Analysis (Order the values of dimensions  by measure) 
	-- Top N performers | Bottom NPerformers
-- ==========================================================================================

-- Which 5 product generates the highest revenue?

SELECT TOP 5
	p.product_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
INNER JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue DESC


-- What are the 5 worst-performing products in terms of sales
-- Option 1
SELECT TOP 5
	p.product_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
INNER JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
ORDER BY total_revenue ASC

-- Option 2 using RANK function
SELECT *
FROM(
SELECT 
	p.product_name,
	SUM(f.sales_amount) AS total_revenue,
	ROW_NUMBER() OVER(ORDER BY SUM(f.sales_amount) DESC ) AS rank_products
FROM gold.fact_sales f
INNER JOIN gold.dim_products p
ON p.product_key = f.product_key
GROUP BY p.product_name
)t 
WHERE rank_products <= 5



-- Who are 3 top ranking customers
SELECT TOP 3
	c.customer_key,
	c.first_name,
	c.last_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
INNER JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue DESC

-- Who are the 3 lowest performing customers
SELECT TOP 3
	c.customer_key,
	c.first_name,
	c.last_name,
	SUM(f.sales_amount) AS total_revenue
FROM gold.fact_sales f
INNER JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_revenue ASC


-- Customers with the fewest orders placed
SELECT TOP 3
	c.customer_key,
	c.first_name,
	c.last_name,
	COUNT(DISTINCT f.order_number) AS total_orders
FROM gold.fact_sales f
INNER JOIN gold.dim_customers c
ON c.customer_key = f.customer_key
GROUP BY c.customer_key, c.first_name, c.last_name
ORDER BY total_orders ASC