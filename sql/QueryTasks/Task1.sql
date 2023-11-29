-- Task 1: Counts the total number of stores in each country
SELECT 
    country_code,
    COUNT(*) AS total_no_stores
FROM 
    dim_store_details
GROUP BY 
    country_code;