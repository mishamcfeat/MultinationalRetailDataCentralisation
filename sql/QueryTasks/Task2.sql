-- Task 2: Lists locations with at least 10 stores, ordered by the number of stores descending
SELECT 
    locality,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    locality
HAVING 
    COUNT(*) >= 10
ORDER BY 
    total_no_stores DESC;
