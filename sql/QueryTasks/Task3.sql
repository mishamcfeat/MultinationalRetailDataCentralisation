-- Task 3: Identifies the top 6 months with the highest sales
SELECT 
    dim_date_times.month,
    ROUND(SUM(orders_table.product_quantity * dim_products.product_price_pounds)::numeric, 2) AS total_sales
FROM 
    orders_table
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
    dim_date_times.month
ORDER BY 
    total_sales DESC
LIMIT 6;