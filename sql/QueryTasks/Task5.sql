-- Task 5: Calculates the percentage of total sales contributed by each store type
WITH sales_cte AS (
    SELECT
        SUM(dim_products.product_price_pounds * orders_table.product_quantity)::numeric AS total_sales
    FROM 
        orders_table
    JOIN 
        dim_products ON orders_table.product_code = dim_products.product_code
)

SELECT 
    dim_store_details.store_type,
    ROUND(SUM(dim_products.product_price_pounds * orders_table.product_quantity)::numeric, 2) AS total_sales,
    ROUND((SUM(dim_products.product_price_pounds * orders_table.product_quantity)::numeric * 100) / (SELECT total_sales FROM sales_cte), 2) AS "percentage_total(%)"
FROM 
    orders_table
JOIN 
    dim_store_details ON orders_table.store_code = dim_store_details.store_code
JOIN 
    dim_products ON orders_table.product_code = dim_products.product_code
GROUP BY 
    dim_store_details.store_type;