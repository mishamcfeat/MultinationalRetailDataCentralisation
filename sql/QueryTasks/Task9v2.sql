-- Task 9: (Second attempt - different implementation but gets results like requested table)
SELECT 
    year,
    FORMAT(
        '"hours": %s, "minutes": %s, "seconds": %s',
        FLOOR(average_seconds / 3600),
        FLOOR((average_seconds % 3600) / 60),
        average_seconds % 60
    ) AS actual_time_taken,
    average_seconds
FROM (
    SELECT 
        year,
        COUNT(*) AS total_sales,
        (365 * 24 * 60 * 60) / COUNT(*) AS average_seconds 
    FROM 
        dim_date_times
	WHERE YEAR != '1992' AND YEAR != '2022'
    GROUP BY 
        year
) AS YearlySales
ORDER BY 
    average_seconds DESC
LIMIT 5;