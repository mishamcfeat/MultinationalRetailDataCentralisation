-- Task 7: Sums the staff numbers for each country and orders by the total descending
SELECT 
    country_code,
    SUM(staff_numbers) AS total_staff_numbers
FROM 
    dim_store_details
GROUP BY 
    country_code
ORDER BY 
    total_staff_numbers DESC;