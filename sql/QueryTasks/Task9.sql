-- Task 9: Calculates the average time taken to make sales in each year, presenting results in JSON format
WITH TimeDifferences AS (
    SELECT 
        year,
        LEAD(timestamp) OVER (PARTITION BY year ORDER BY timestamp) - timestamp AS time_diff
    FROM 
        dim_date_times
),

AvgTimeDifferences AS (
    SELECT 
        year,
        AVG(time_diff) AS avg_time_diff
    FROM 
        TimeDifferences
    WHERE 
        time_diff IS NOT NULL
    GROUP BY 
        year
)

SELECT 
    year,
    JSON_BUILD_OBJECT(
        'hours', EXTRACT(HOUR FROM avg_time_diff), 
        'minutes', EXTRACT(MINUTE FROM avg_time_diff), 
        'seconds', EXTRACT(SECOND FROM avg_time_diff)
    ) AS actual_time_taken
FROM 
    AvgTimeDifferences
ORDER BY 
    avg_time_diff DESC;