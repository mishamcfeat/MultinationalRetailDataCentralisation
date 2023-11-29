-- Task 8: Ranks store types in Germany by total sales in ascending order
SELECT 
    dim_store_details.store_type,
    ROUND(SUM(dim_products.product_price_pounds * orders_table.product_quantity)::numeric, 2) AS total_sales
FROM 
    orders_table
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
WHERE 
    dim_store_details.country_code = 'DE'
GROUP BY 
    dim_store_details.store_type
ORDER BY 
    total_sales ASC;