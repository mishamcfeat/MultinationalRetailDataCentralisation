-- Task 4: Compares the number and quantity of sales from online and offline stores
SELECT 
    'Web' AS location,
    COUNT(*) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count
FROM 
    orders_table
WHERE 
    store_code = 'WEB-1388012W'

UNION ALL

SELECT 
    'Offline' AS location,
    COUNT(*) AS numbers_of_sales,
    SUM(product_quantity) AS product_quantity_count
FROM 
    orders_table
WHERE 
    store_code != 'WEB-1388012W';