-- Task 6: Finds the top 10 months across all years with the highest cost of sales
SELECT 
    year,
    month,
    ROUND(SUM(dim_products.product_price_pounds * orders_table.product_quantity)::numeric, 2) AS total_sales
FROM 
    orders_table
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
JOIN 
    dim_date_times ON orders_table.date_uuid = dim_date_times.date_uuid
GROUP BY 
    year, month
ORDER BY 
    total_sales DESC
LIMIT 10;